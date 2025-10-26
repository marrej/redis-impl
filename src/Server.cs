using System.Data.SqlTypes;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using RedisImpl;

class Redis
{
    public static void Main(string[] args)
    {
        StartupFlags flags = Redis.GetFlags(args);

        // We can test the connection using netcat, e.g. `echo "foo" | nc localhost 6379`
        // Or better `echo -e "PING\nPING" | redis-cli`
        Console.WriteLine("Server Starting");

        var activePort = flags.Port ?? 6379;
        TcpListener server = new(IPAddress.Any, activePort);
        server.Start();
        Storage storage = new();
        Parser parser = new();

        // Create a bridge with its own interpreter to process the replica commands
        var bridge = new MasterReplicaBridge { Port = activePort };
        Thread bridgeConn = new Thread(BridgeLoadingLoop);
        bridgeConn.Start(new CliInput { Id = 0, Parser = parser, Storage = storage, Bridge = bridge, Flags = flags });

        Console.WriteLine("Accepting at " + activePort);

        // A communication loop listenting to socket exchanges
        var id = 0;
        while (true)
        {
            Socket socket = server.AcceptSocket(); // wait for client
            Console.WriteLine("Client connected"); // Accepts a socket to connect
                                                   // Resolves all commands sent from the socket in succession

            Thread conn = new Thread(ThreadLoop);
            conn.Start(new CliInputWithSocket { Socket = socket, Id = id, Parser = parser, Storage = storage, Bridge = bridge, Flags = flags });
            id++;
        }
    }

    public static void BridgeLoadingLoop(object data)
    {
        CliInput c = (CliInput)data;
        Interpreter replicaInterpreter = new() { Storage = c.Storage, Id = -1, Bridge = c.Bridge };
        c.Bridge.SetRole(c.Flags.Master, (string message) =>
        {
            Console.WriteLine(message);
            var p = c.Parser.ParseCommandLists(message);
            if (p == null) { return; }
            foreach (var comm in p)
            {
                var i = replicaInterpreter.Execute(comm);
            }
        });
    }

    public static StartupFlags GetFlags(string[] args)
    {
        StartupFlags flags = new();
        for (var i = 0; i < args.Length; i++)
        {
            if (args[i] == "--port" && args.Length - 1 >= i + 1)
            {
                i++;
                flags.Port = Int32.Parse(args[i]);
            }
            else if (args[i] == "--replicaof" && args.Length - 1 >= i + 1)
            {
                i++;
                var conn = args[i].Split(" ");
                if (conn.Length != 2)
                {
                    throw new Exception("Invalid conn string");
                }
                flags.Master = new MasterInfo { Ip = conn[0], Port = Int32.Parse(conn[1]) };
            }
        }
        return flags;
    }


    public static void ThreadLoop(object data)
    {
        CliInputWithSocket c = (CliInputWithSocket)data;
        Console.WriteLine("Connected thread {0}", c.Id);
        Interpreter interpreter = new() { Storage = c.Storage, Id = c.Id, Bridge = c.Bridge };

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
                // Parses joined messages (e.g. multiple SET commands together)
                var p = c.Parser.ParseCommandLists(message);
                if (p == null) { continue; }
                foreach (var comm in p)
                {
                    var i = interpreter.Execute(comm);
                    // Responds using SimpleString which is. "+`${ret}`\r\n" at all times
                    var response = Encoding.ASCII.GetBytes(i);
                    c.Socket.Send(response);
                    if (interpreter.StartReplication)
                    {
                        var sendStringResponse = (string res) =>
                        {
                            c.Socket.Send(Encoding.ASCII.GetBytes(res));
                        };

                        Console.WriteLine("starting replication");
                        var (len, rdb) = c.Bridge.GetRdb(interpreter.NewReplica);
                        sendStringResponse(len);
                        c.Socket.Send(rdb);
                        c.Bridge.StartConsuming(sendStringResponse);
                        continue;
                    }
                }
            }
            catch (Exception e)
            {
                var r = Encoding.ASCII.GetBytes(Types.GetSimpleString("ERROR didn't parse correctly"));
                c.Socket.Send(r);
                continue;
            }
        }
    }
}

class StartupFlags
{
    public int? Port;

    public MasterInfo? Master;
}

class CliInput
{
    required public int Id;

    required public Parser Parser;
    required public Storage Storage;

    required public MasterReplicaBridge Bridge;

    required public StartupFlags Flags;
}

class CliInputWithSocket : CliInput
{
    required public Socket Socket;
}

