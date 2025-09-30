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
    readonly Dictionary<string, List<string>> L = [];
    readonly Dictionary<string, string> S = [];
    readonly Dictionary<string, DateTime> TTLs = [];

    // Called on every interaction with the Storage to evaluate whether it shoudl evacuate a property.
    // Running on every call is costly. Might be better to evaluate just the key on call
    // TODO: run a ticker in 10sec increments async to cleanup.
    private void EvaluateTTL(string key)
    {
        if (TTLs.ContainsKey(key) && TTLs[key] < DateTime.Now)
        {
            TTLs.Remove(key);
            S.Remove(key);
        }
    }

    public string Get(string key)
    {
        this.EvaluateTTL(key);
        if (!S.TryGetValue(key, out string? value))
        {
            throw new Exception("Value not found");
        }
        return value;
    }

    public void Set(string key, string val, SetOptions options)
    {
        this.EvaluateTTL(key);
        if (options == null)
        {
            S[key] = val;
            return;
        }

        var insertedValue = false;
        S.TryGetValue(key, out string? value);
        // Conditionally inserts the value. If not condition is set then inserts
        if (value != null && options.AddOnlyIfAlreadyDefined)
        {
            insertedValue = true;
            S[key] = val;
        }
        else if (value == null && options.AddOnlyIfNotDefined)
        {
            insertedValue = true;
            S[key] = val;
        }
        else if (!options.AddOnlyIfAlreadyDefined && !options.AddOnlyIfNotDefined)
        {
            insertedValue = true;
            S[key] = val;
        }
        else
        {
            throw new Exception("Can't insert value");
        }

        if (insertedValue && options.TTL != null)
        {
            var newTtl = DateTime.Now;
            switch (options.TTL.Type.ToUpper())
            {
                case "EX":
                    newTtl = DateTime.Now.AddSeconds(options.TTL.Time);
                    break;
                case "PX":
                    newTtl = DateTime.Now.AddMilliseconds(options.TTL.Time);
                    break;
                case "EXAT":
                    newTtl = DateTimeOffset.FromUnixTimeSeconds(options.TTL.Time).DateTime;
                    break;
                case "PXAT":
                    newTtl = DateTimeOffset.FromUnixTimeMilliseconds(options.TTL.Time).DateTime;
                    break;
                case "KEEPTTL":
                    // Since we are keeping last TTL, we don't need to do anything.
                    break;
            }
            TTLs[key] = newTtl;

        }
        else
        {
            TTLs.Remove(key);
        }
    }

    public int Rpush(string list, string input)
    {
        if (!L.ContainsKey(list))
        {
            L[list] = [];
        }
        L[list].Add(input);
        return L[list].Count;
    }

    // Doesn't error out but instead returns empty array
    public List<string> Lrange(string list, int start, int stop)
    {
        L.TryGetValue(list, out List<string>? arr);
        if (arr == null)
        {
            return [];
        }

        // - index allows to get to the end of array.
        if (stop < 0)
        {
            stop = arr.Count + stop;
        }

        // Avoids overruning the array
        start = Math.Min(arr.Count - 1, start);
        stop = Math.Min(arr.Count - 1, stop);

        if (start > stop)
        {
            return [];
        }
        return arr[start..(stop + 1)];
    }
}

class TTLOptions
{ 
    required public int Time;

    // https://redis.io/docs/latest/commands/set/#options
    // E.g. EX, PX...
    required public string Type;
}

class SetOptions
{
    public TTLOptions? TTL;

    public bool AddOnlyIfAlreadyDefined;
    public bool AddOnlyIfNotDefined;
}

class Interpreter
{
    required public Storage Storage;
    public string Interpret(List<string> p)
    {
        var command = p.Count > 0 ? p[0] : "ERROR";
        var arguments = p.Count > 1 ? p[1..] : [];
        return command.ToUpper() switch
        {
            // TODO: refactor. Instead of returning direct value, after interpretation, this should just return a Command object that we can execute in some way?
            "PING" => Types.GetSimpleString("PONG"),
            "ECHO" => this.Echo(arguments),
            "GET" => this.Get(arguments),
            "SET" => this.Set(arguments),
            "RPUSH" => this.Rpush(arguments),
            "LRANGE" => this.Lrange(arguments),
            _ => Types.GetSimpleString("ERROR no command specified"),
        };
    }

    public string Echo(List<string> arguments)
    {
        return Types.GetBulkString(arguments);
    }


    // https://redis.io/docs/latest/commands/set/#options
    public string Set(List<string> arguments)
    {
        if (arguments.Count < 2)
        {
            return Types.GetSimpleString("ERROR invalid parameter count");
        }
        var key = arguments[0];
        var val = arguments[1];
        string? measurement = null;
        int time = 0;
        bool retOldKey = false;
        string? onlySetIf = null;

        for (var i = 2; i < arguments.Count; i++)
        {
            var command = arguments[i];
            switch (command.ToUpper())
            {
                case "NX":
                case "XX":
                    onlySetIf = command;
                    break;
                case "GET":
                    retOldKey = true;
                    break;
                case "EX":
                case "PX":
                case "EXAT":
                case "PXAT":
                    if (measurement != null)
                    {
                        return Types.GetSimpleString("ERROR ttl already set");
                    }
                    measurement = command;
                    if (arguments.Count < i + 1)
                    {
                        return Types.GetSimpleString("ERROR missing time variable");
                    }
                    time = int.Parse(arguments[i + 1]);
                    i++;
                    break;
                case "KEEPTTL":
                    if (measurement != null)
                    {
                        return Types.GetSimpleString("ERROR ttl already set");
                    }
                    measurement = command;
                    break;
                default:
                    continue;
            }
        }

        try
        {
            var setOptions = new SetOptions
            {
                AddOnlyIfAlreadyDefined = onlySetIf != null && onlySetIf == "XX",
                AddOnlyIfNotDefined = onlySetIf != null && onlySetIf == "NX",
            };
            if (measurement != null)
            {
                setOptions.TTL = new TTLOptions
                {
                    Type = measurement,
                    Time = time,
                };
            }

            var prevVal = this.Get([key]);

            this.Storage.Set(key, val, setOptions);
            if (retOldKey)
            {
                return prevVal;
            }
            return Types.GetSimpleString("OK");
        }
        catch (Exception)
        {
            return Types.GetSimpleString("ERROR setting val");
        }
    }

    public string Get(List<string> arguments)
    {
        if (arguments.Count < 1)
        {
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

    public string Rpush(List<string> arguments)
    {
        if (arguments.Count < 2)
        {
            return Types.GetSimpleString("ERROR invalida arg count");
        }
        var listLength = 0;
        var list = arguments[0];
        for (var i = 1; i < arguments.Count; i++)
        {
            var item = arguments[i];
            listLength = this.Storage.Rpush(list, item);
        }
        return Types.GetInteger(listLength);
    }

    // https://redis.io/docs/latest/commands/lrange/
    public string Lrange(List<string> arguments)
    {
        if (arguments.Count != 3)
        {
            return Types.GetSimpleString("ERROR incorrect argument count");
        }

        // INVARIANT: all args are provided
        var list = arguments[0];
        var start = Int32.Parse(arguments[1]);
        var end = Int32.Parse(arguments[2]);
        return Types.GetStringArray(Storage.Lrange(list, start, end));
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
            return bulkString + (-1).ToString() + "\r\n";
        }

        foreach (var a in inputs)
        {
            var len = a.Length + "\r\n";
            var content = a + "\r\n";
            bulkString = bulkString + len + content;
        }
        return bulkString;
    }

    // https://redis.io/docs/latest/develop/reference/protocol-spec/#integers
    public static string GetInteger(int input)
    {
        return ":" + input.ToString() + "\r\n";
    }

    public static string GetStringArray(List<string> inputs)
    {
        var ret = "*" + inputs.Count.ToString() + "\r\n";
        for (var i = 0; i < inputs.Count; i++)
        {
            ret += Types.GetBulkString([inputs[i]]);
        }
        return ret;
    }
    
    public static string GetIntArray(List<int> inputs)
    {
        var ret = "*"+inputs.Count.ToString()+"\r\n";
        for (var i = 0; i < inputs.Count; i++)
        {
            ret += Types.GetInteger(inputs[i]);
        }
        return ret;
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