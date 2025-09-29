using System.Data.SqlTypes;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;

// We can test the connection using netcat, e.g. `echo "foo" | nc localhost 6379`
// Or better `echo -e "PING\nPING" | redis-cli`

Console.WriteLine("Server Starting");

TcpListener server = new(IPAddress.Any, 6379);
server.Start();
Storage storage = new();
Console.WriteLine("Accepting at 6379");

// A communication loop listenting to socket exchanges
var id = 0;
while (true)
{
    Socket socket = server.AcceptSocket(); // wait for client
    Console.WriteLine("Client connected"); // Accepts a socket to connect
    // Resolves all commands sent from the socket in succession

    Thread conn = new Thread(ThreadLoop);
    conn.Start(new CliInput { Socket = socket, Id = id, Storage = storage });
    id++;
}

void ThreadLoop(object data)
{
    CliInput c = (CliInput)data;
    Console.WriteLine("Connected thread {0}", c.Id);
    var parser = new Parser { };
    var interpreter = new Interpreter { Storage = c.Storage};

    while (true)
    {
        byte[] buffer = new byte[1024];
        var bytes = c.Socket.Receive(buffer);
        // Disconnect if no more requests are received (the client errors out)
        if (bytes == 0)
        {
            break;
        }
        var message = Encoding.ASCII.GetString(buffer);
        try
        {
            var p = parser.Parse(message);
            var i = interpreter.Interpret(p);
            Console.WriteLine(i);
            // Responds using SimpleString which is. "+`${ret}`\r\n" at all times
            var response = Encoding.ASCII.GetBytes(i);
            c.Socket.Send(response);
            Console.WriteLine("Response Sent");
        }
        catch (Exception e)
        {
            var r = Encoding.ASCII.GetBytes(Types.GetSimpleString("ERROR didn't parse correctly"));
            c.Socket.Send(r);
            continue;
        }
    }
}

class CliInput
{
    required public int Id { get; set; }
    required public Socket Socket { get; set; }

    required public Storage Storage { get; set; }
}

class Storage
{
    readonly Dictionary<string, string> s = [];

    public string Get(string key)
    {
        if (!s.TryGetValue(key, out string? value))
        {
            throw new Exception("Value not found");
        }
        return value;
    }

    public void Set(string key, string val)
    {
        // Currently always overwrites the past input
        s[key] = val;
    }
}

class Interpreter
{
    required public Storage Storage;
    public string Interpret(List<string> p)
    {
        var command = p.Count > 0 ? p[0] : "ERROR";
        var arguments = p.Count > 1 ? p[1..] : [];
        return command switch
        {
            // TODO: refactor. Instead of returning direct value, after interpretation, this should just return a Command object that we can execute in some way?
            "PING" => Types.GetSimpleString("PONG"),
            "ECHO" => this.Echo(arguments),
            "GET" => this.Get(arguments),
            "SET" => this.Set(arguments),
            _ => Types.GetSimpleString("ERROR no command specified"),
        };
    }

    public string Echo(List<string> arguments)
    {
        return Types.GetBulkString(arguments);
    }


    public string Set(List<string> arguments)
    {
        if (arguments.Count < 2)
        {
            return Types.GetSimpleString("ERROR invalid parameter count");
        }
        var key = arguments[0];
        var val = arguments[1];
        try
        {
            this.Storage.Set(key, val);
            return Types.GetSimpleString("OK");
        }
        catch (Exception)
        { 
            return Types.GetSimpleString("ERROR setting val");
        }
    }

    public string Get(List<string> arguments)
    {
        if (arguments.Count < 1) {
            return Types.GetSimpleString("ERROR missing key");
        }
        try
        {
            var val = this.Storage.Get(arguments[0]);
            return Types.GetBulkString([val]);
        }
        catch (Exception)
        {
            return Types.GetBulkString(null);
        }
    }
}

class Types
{
    // https://redis.io/docs/latest/develop/reference/protocol-spec/#simple-strings
    public static string GetSimpleString(string i)
    {
        return "+" + i + "\r\n";
    }

    // https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-strings
    public static string GetBulkString(List<string>? inputs)
    {
        var bulkString = "$";
        if (inputs == null)
        { 
            return bulkString+(-1).ToString()+"\r\n";
        }

        foreach (var a in inputs)
        {
            var len = a.Length + "\r\n";
            var content = a + "\r\n";
            bulkString = bulkString + len + content;
        }
        return bulkString;
    }
}

class Parser
{
    public List<string> Parse(string input)
    {
        // The input contains
        // - *paramcount\r\n
        // - $length\r\n
        // - string\r\n
        var lines = input.Split("\r\n");
        try
        {
            int paramCount = Int32.Parse(lines[0].Substring(1));
            List<string> p = [];
            for (var i = 0; i < paramCount; i++)
            {
                p.Add(lines[2 + (i * 2)]);
            }
            return p;
        }
        catch (Exception e)
        {
            Console.WriteLine("Didn't parse input correctly");
            throw e;
        }
    }
}