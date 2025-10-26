// This class handles the async writing and governs
// all operations related to the master-replica communication
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using RedisImpl;

class MasterReplicaBridge
{
    // Contains all waiting replicas
    private LinkedList<Semaphore> SemaphoreQueue = new();

    // Contains all edit commands that were consumed by the master
    private LinkedList<string> CommandQueue = new();

    private Dictionary<Int128, ReplicaConn> Replicas = new();
    public bool IsMaster = false;

    // Data set only if the node is Master;
    public string? ReplId;

    public Int128? ProducedBytes;

    private MasterInfo? MasterConn;

    public int Port;

    // Only when node is replica
    // References to the consumed Command Stream 
    private string? MasterReplId;
    // The last consumed offset from the Master command stream
    private Int128 ConsumedBytes = -1;

    public void SetRole(MasterInfo? info, Action<string> processMessage)
    {
        if (info == null)
        {
            this.IsMaster = true;
            this.ProducedBytes = 0;
            // Extend by "repl" - so that the length is 40 per requirement
            this.ReplId = System.Guid.NewGuid().ToString().Replace("-", "X") + "repl";
        }
        else
        {
            this.MasterConn = info;
            // We will need to use this also on reconnect
            this.ConnectToMaster(info, processMessage);
        }
    }

    // "Master" -> insters commands to be consumed
    public void QueueCommand(string command, List<string> arguments)
    {
        // var commandString = arguments.Aggregate(command, (agg, next) => agg + " " + next);
        var args = arguments;
        args.Insert(0, command);
        CommandQueue.AddLast(Types.GetStringArray(args));
        while (this.SemaphoreQueue.Count > 0)
        {
            this.SemaphoreQueue.First?.Value.Release();
            this.SemaphoreQueue.RemoveFirst();
        }
    }

    private void ConnectToMaster(MasterInfo info, Action<string> processMessage)
    {
        TcpClient client = new();
        client.Connect(info.Ip, info.Port);

        var stream = client.GetStream();
        List<List<string>> reqs = [
            ["PING"],
            ["REPLCONF", "listening-port", this.Port.ToString()],
            ["REPLCONF", "capa", "psync2"],
            ["PSYNC", this.MasterReplId ?? "?", this.ConsumedBytes.ToString()],
            ];
        // Initiate connection - first just by sending Ping and then breaking the socket.
        var isLoadingFromRdb = false;
        foreach (var r in reqs)
        {
            byte[] sendBuffer = Encoding.UTF8.GetBytes(Types.GetStringArray(r));
            stream.Write(sendBuffer);

            byte[] buffer = new byte[1024];
            stream.Socket.Receive(buffer);
            if (!stream.Socket.Connected)
            {
                return;
            }
            var message = Encoding.ASCII.GetString(buffer);

            if (r[0] == "PSYNC" && message.StartsWith("+FULLRESYNC"))
            {
                isLoadingFromRdb = true;
            }
        }
        var i = 0;
        while (true)
        {
            byte[] buffer = new byte[1024];
            stream.Socket.Receive(buffer);
            if (i == 1 && isLoadingFromRdb)
            {
                Console.WriteLine("RDB");
                // TODO: process the RDB
                Console.WriteLine();
                // Start preparsing
                processMessage(System.Text.Encoding.Default.GetString(buffer));
                i++;
                continue;
            }
            Console.WriteLine("Message processing");
            var message = Encoding.ASCII.GetString(buffer);
            if (message.Replace("\u0000", "").Length == 0)
            {
                Console.WriteLine("Empty message");
                return;
            }
            processMessage(message);
            i++;
        }
    }

    public string GetReplicaInfo()
    {
        var role = "role:" + (this.IsMaster ? "master" : "slave");
        var replId = "master_replid:" + this.ReplId;
        var replOffset = "master_repl_offset:" + this.ProducedBytes.ToString();
        return role + "\n" + replId + "\n" + replOffset + "\n";
    }

    public void AddReplica(ReplicaConn r)
    {
        this.Replicas[r.Port] = r;
    }

    public (string, byte[]) GetRdb(ReplicaConn? conn)
    {
        if (conn == null)
        {
            throw new Exception("ERR Replica not available");
        }
        var emptyBase64 = "UkVESVMwMDEx+glyZWRpcy12ZXIFNy4yLjD6CnJlZGlzLWJpdHPAQPoFY3RpbWXCbQi8ZfoIdXNlZC1tZW3CsMQQAPoIYW9mLWJhc2XAAP/wbjv+wP9aog==";
        var buf = Convert.FromBase64String(emptyBase64);
        return ($"${buf.Length}\r\n", buf);
    }

    public void StartConsuming(Action<string> sendCommand)
    {
        var semaphore = new Semaphore(0, 1);
        LinkedListNode<string>? lastCommandNode = null;
        while (true)
        {
            if (this.CommandQueue.First == null || (lastCommandNode != null &&lastCommandNode?.Next == null))
            {
                this.SemaphoreQueue.AddLast(semaphore);
                semaphore.WaitOne();
                continue;
            }
            if (lastCommandNode?.Next != null)
            {
                lastCommandNode = lastCommandNode.Next;
            }
            else if (lastCommandNode == null)
            {
                lastCommandNode = this.CommandQueue.First;
            }
            sendCommand(lastCommandNode.Value);
        }
    }
}

class MasterInfo
{
    required public string Ip;
    required public int Port;
}

class ReplicaConn
{
    required public int Port;

    public string? MasterReplId;
    // The last consumed offset from the Master command stream
    public Int128 ConsumedBytes = -1;
}