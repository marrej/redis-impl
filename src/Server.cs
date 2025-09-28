using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;

// We can test the connection using netcat, e.g. `echo "foo" | nc localhost 6379`
// Or better `echo -e "PING\nPING" | redis-cli`

Console.WriteLine("Server Starting");

TcpListener server = new TcpListener(IPAddress.Any, 6379);
server.Start();
Console.WriteLine("Accepting at 6379");

// A communication loop listenting to socket exchanges
var id = 0;
while (true)
{
    Socket socket = server.AcceptSocket(); // wait for client
    Console.WriteLine("Client connected"); // Accepts a socket to connect
    // Resolves all commands sent from the socket in succession

    Thread conn = new Thread(ThreadLoop);
    conn.Start(new CliInput { socket = socket, id = id });
    id++;
}

void ThreadLoop(object data)
{
    CliInput c = (CliInput)data;
    Console.WriteLine("Connected thread {0}", c.id);
    var parser = new Parser { };
    var interpreter = new Interpreter { };

    while (true)
    {
        byte[] buffer = new byte[1024];
        var bytes = c.socket.Receive(buffer);
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
            c.socket.Send(response);
            Console.WriteLine("Response Sent");
        }
        catch (Exception e)
        {
            var r = Encoding.ASCII.GetBytes("+ERROR didn't parse correctly\r\n");
            c.socket.Send(r);
            continue;
        }
    }
}

class CliInput
{
    required public int id { get; set; }
    required public Socket socket { get; set; }
}

class Interpreter
{
    public string Interpret(List<string> p)
    {
        var command = p.Count > 0 ? p[0] : "ERROR";
        var arguments = p.Count > 1 ? p.Slice(1, p.Count-1) : new List<string> { };
        switch (command)
        {
            case "PING":
                return "+PONG\r\n";
            case "ECHO":
                return this.Echo(arguments);
            default:
                return "+ERROR no command specified\r\n";
        }
    }

    public string Echo(List<string> arguments)
    {
        // https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-strings
        var bulkString = "$";
        foreach (var a in arguments)
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