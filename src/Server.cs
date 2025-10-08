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
Parser parser = new();
Console.WriteLine("Accepting at 6379");

// A communication loop listenting to socket exchanges
var id = 0;
while (true)
{
    Socket socket = server.AcceptSocket(); // wait for client
    Console.WriteLine("Client connected"); // Accepts a socket to connect
    // Resolves all commands sent from the socket in succession

    Thread conn = new Thread(ThreadLoop);
    conn.Start(new CliInput { Socket = socket, Id = id, Parser = parser, Storage = storage });
    id++;
}

void ThreadLoop(object data)
{
    CliInput c = (CliInput)data;
    Console.WriteLine("Connected thread {0}", c.Id);
    Interpreter interpreter = new() { Storage = c.Storage, Id = c.Id };

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
            var p = c.Parser.Parse(message);
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

    required public Parser Parser { get; set; }
    required public Storage Storage { get; set; }
}

class WaitingTicket
{
    required public int ThreadId;
    public DateTime? WaitUntil;
    // Semaphore shared between multiple tickets of the same call
    required public Semaphore Semaphore;

    required public bool Released = false;

    // List from where the value was popped
    public string? List;
    // Value that was popped

    public string? Value;
}

// Contains all threads that are awaiting access to a List
class BlockingQueue
{
    public readonly LinkedList<WaitingTicket> Tickets = [];
}

class Storage
{
    readonly Dictionary<string, LinkedList<string>> L = [];
    readonly Dictionary<string, BlockingQueue> LQ = [];
    // Sync property checking whether there is Update list in progress to avoid multi triggers on parallel pushes.
    readonly HashSet<string> ListUpdatesInProgress = [];
    // Mutext used for popping data off the lists;
    readonly private Mutex BlockingPopMutex = new();
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

    // TODO: Add mutex to avoid running this operation multiple times while the first one may be still executed
    public void DidUpdateList(string list)
    {

        LQ.TryGetValue(list, out BlockingQueue? bq);
        if (bq == null || bq?.Tickets.Count <= 0) { return; }

        L.TryGetValue(list, out LinkedList<string>? storage);
        if (storage == null || storage?.Count <= 0) { return; }

        if (ListUpdatesInProgress.Contains(list)) { return; }
        ListUpdatesInProgress.Add(list);
        while ((storage?.Count ?? 0) > 0)
        {
            var waiting = bq?.Tickets.First?.Value;
            var last = bq?.Tickets.Last?.Value;
            // Avoid running the queue if the last was already released
            if (waiting == null || last == null || last.Released) { break; }
            // Skip to the first waiting which was not yet released;
            if (waiting.Released)
            {
                bq?.Tickets.RemoveFirst();
                continue;
            }
            var item = this.Lpop(list, 1); ;
            if (item == null || item.Count < 1) { break; }
            waiting.Released = true;
            waiting.List = list;
            waiting.Value = item[0];
            waiting.Semaphore.Release();
            bq?.Tickets.RemoveFirst();
        }
        if (bq?.Tickets.Count == 0)
        {
            LQ.Remove(list);
        }
        ListUpdatesInProgress.Remove(list);
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
        L[list].AddLast(input);
        return L[list].Count;
    }

    public int Lpush(string list, List<string> items)
    {
        if (!L.ContainsKey(list))
        {
            L[list] = [];
        }

        foreach (var i in items)
        {
            L[list].AddFirst(i);
        }
        return L[list].Count;
    }

    public int Llen(string list)
    {
        L.TryGetValue(list, out LinkedList<string>? arr);
        return arr == null ? 0 : arr.Count;
    }

    // Doesn't error out but instead returns empty array
    public List<string> Lrange(string list, int start, int stop)
    {
        L.TryGetValue(list, out LinkedList<string>? arr);
        if (arr == null)
        {
            return [];
        }

        // - index allows to get to the end of array.
        if (start < 0)
        {
            start = Math.Max(arr.Count + start, 0);
        }
        if (stop < 0)
        {
            stop = Math.Max(arr.Count + stop, 0);
        }

        // Avoids overruning the array
        start = Math.Min(arr.Count - 1, start);
        stop = Math.Min(arr.Count - 1, stop);

        if (start > stop)
        {
            return [];
        }

        // Retrieve the slice from the LinkedList
        List<string> slice = [];
        var index = 0;
        foreach (var item in arr)
        {
            if (index > stop)
            {
                break;
            }
            if (index >= start && index <= stop)
            {
                slice.Add(item);
            }
            index++;
        }
        return slice;
    }

    public List<string>? Lpop(string list, int count)
    {
        L.TryGetValue(list, out LinkedList<string>? arr);
        if (arr == null || arr.Count == 0)
        {
            return null;
        }

        // Iterate over list, while an item can be popped.
        List<string> popped = [];
        var next = arr.First;
        for (var i = 0; i < count; i++)
        {
            if (next == null)
            {
                break;
            }
            popped.Add(next.Value);
            next = next.Next;
            arr.RemoveFirst();
        }
        return popped;
    }

    // Returns List + value that was popped
    // QQ: should we block if there is some thread that is trying to read the specific list which we want to pop?
    // Otherwise there is possible race between the mutexed and the non mutexted thread??
    public List<string>? Blpop(List<string> lists, int id, int timeout)
    {
        // TODO: wrap the semaphore with an object containing also "timeout passed"
        Semaphore callBlocker = new(initialCount: 0, maximumCount: 1);
        var ticket = new WaitingTicket { ThreadId = id, Semaphore = callBlocker, Released = false };
        if (timeout > 0)
        {
            ticket.WaitUntil = DateTime.Now.AddSeconds(timeout);
        }

        // Block the queue creation to avoid ticket dissapearance and racing between blocking calls.
        BlockingPopMutex.WaitOne();
        foreach (var l in lists)
        {
            // If others are waiting, then add ourselves to the lists and wait
            LQ.TryGetValue(l, out BlockingQueue? blockingQueue);
            if (blockingQueue == null || blockingQueue.Tickets.Count <= 0 || blockingQueue.Tickets?.Last?.Value.Released == true)
            {
                var val = this.Lpop(l, 1);
                if (val != null)
                {
                    // TODO: remove all the past tickets (Connect the parent to the Next in the List)
                    BlockingPopMutex.ReleaseMutex();
                    return [l, val[0]];
                }
            }

            // TODO: wrap with mutex to avoid overriding the blocking queue
            var bq = blockingQueue ?? new BlockingQueue { };
            if (blockingQueue == null)
            {
                LQ.Add(l, bq);
            }
            bq.Tickets.AddLast(ticket);
        }
    
        BlockingPopMutex.ReleaseMutex();

        // Wait for the ticket to be resolved
        callBlocker.WaitOne();
        // TODO: check whether was timeouted by checking a timeout dict, then we shoudl skip execution

        if (ticket.List == null || ticket.Value == null)
        {
            return null;
        }
        return [ticket.List, ticket.Value];
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
    // Id of the current thread.
    required public int Id;
    public string Interpret(List<string> p)
    {
        var command = p.Count > 0 ? p[0] : "ERROR";
        var arguments = p.Count > 1 ? p[1..] : [];
        return command.ToUpper() switch
        {
            // TODO: refactor. Instead of returning direct value, after interpretation, this should just return a Command object that we can execute in some way?
            "PING" => Types.GetSimpleString("PONG"),
            "ECHO" => this.Echo(arguments),
            // Item actions
            "GET" => this.Get(arguments),
            "SET" => this.Set(arguments),
            // List actions
            "RPUSH" => this.Rpush(arguments),
            "LRANGE" => this.Lrange(arguments),
            "LPUSH" => this.Lpush(arguments),
            "LLEN" => this.Llen(arguments),
            "LPOP" => this.Lpop(arguments),
            "BLPOP" => this.Blpop(arguments),
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
        this.Storage.DidUpdateList(list);
        // TODO: If no thread is processing, release, otherwise do nothing.
        return Types.GetInteger(listLength);
    }

    // https://redis.io/docs/latest/commands/lpush/
    public string Lpush(List<string> arguments)
    {
        if (arguments.Count < 2)
        {
            return Types.GetSimpleString("ERROR incorrect argument count");
        }
        var list = arguments[0];
        var items = arguments[1..];
        var listLength = this.Storage.Lpush(list, items);
        this.Storage.DidUpdateList(list);
        // TODO: If no thread is processing, release, otherwise do nothing.
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

    // https://redis.io/docs/latest/commands/llen/
    public string Llen(List<string> arguments)
    {
        if (arguments.Count != 1)
        {
            return Types.GetSimpleString("ERROR incorrect argument count");
        }
        return Types.GetInteger(Storage.Llen(arguments[0]));
    }

    // https://redis.io/docs/latest/commands/blpop/
    public string Blpop(List<string> arguments)
    {
        if (arguments.Count < 2)
        {
            return Types.GetSimpleString("ERROR incorrect argument count");
        }
        // BLPOP key [key ...] timeout
        var lists = arguments[0..(arguments.Count - 1)];
        var timeout = Int32.Parse(arguments[^1]);
        var ret = this.Storage.Blpop(lists, this.Id, timeout);

        if (ret == null)
        {
            return Types.GetBulkString(null);
        }
        return Types.GetStringArray(ret);
    }

    public string Lpop(List<string> arguments)
    {
        if (arguments.Count < 1)
        {
            return Types.GetSimpleString("ERROR incorrect argument count");
        }
        var list = arguments[0];

        // Always pops at least 1 item
        var popFirst = arguments.Count != 2;
        var count = Math.Max(popFirst ? 1 : Int32.Parse(arguments[1]), 1);
        var popped = Storage.Lpop(list, count);
        if (popped == null)
        {
            return Types.GetBulkString(popped);
        }
        if (popFirst)
        {
            return Types.GetBulkString(popped);
        }
        return Types.GetStringArray(popped);
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
        if (inputs.Count == 0)
        {
            return bulkString + "0\r\n\r\n";
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