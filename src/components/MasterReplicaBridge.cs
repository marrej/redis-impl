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
    private LinkedList<QueuedCommand> CommandQueue = new();

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
    public Int128 ConsumedBytes = -1;

    public void SetRole(MasterInfo? info, Func<string, List<string>?> processMessage)
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
        this.QueueCommand(command, arguments);
    }
    public void QueueCommand(string command, List<string> arguments, SemaphoredCounter? semaphore)
    {
        // var commandString = arguments.Aggregate(command, (agg, next) => agg + " " + next);
        var args = arguments;
        args.Insert(0, command);
        CommandQueue.AddLast(new QueuedCommand { Cmd = Types.GetStringArray(args), Semaphore = semaphore });
        while (this.SemaphoreQueue.Count > 0)
        {
            this.SemaphoreQueue.First?.Value.Release();
            this.SemaphoreQueue.RemoveFirst();
        }
    }

    private void ConnectToMaster(MasterInfo info, Func<string, List<string>?> processMessage)
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
            Console.WriteLine("Waiting for next commands");
            stream.Socket.Receive(buffer);
            if (i == 0 && isLoadingFromRdb)
            {
                Console.WriteLine("RDB len");
                stream.Socket.Send(Encoding.ASCII.GetBytes("RDB len received"));
                i++;
                continue;
            }
            if (i == 1 && isLoadingFromRdb)
            {
                Console.WriteLine("RDB");
                // TODO: process the RDB
                // Start preparsing
                processMessage(System.Text.Encoding.Default.GetString(buffer));
                i++;
                stream.Socket.Send(Encoding.ASCII.GetBytes("RDB received"));
                continue;
            }
            Console.WriteLine("Message processing--");
            var message = Encoding.ASCII.GetString(buffer);
            if (message.Replace("\u0000", "").Length == 0)
            {
                Console.WriteLine("Empty message");
                return;
            }
            // Respond back to replconf acks
            var responses = processMessage(message) ?? [];
            if (responses.Count == 0)
            { 
                stream.Socket.Send(Encoding.ASCII.GetBytes("EMPTY RESPONSE"));  
            }
            foreach (var r in responses)
            {
                stream.Socket.Send(Encoding.ASCII.GetBytes(r));
            }
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

    /*
    * Consumes the write commands if they are ready on the Command Queue.
    * Whenever replica is waiting for further commands to consume, it adds itself to the semaphoreQueue.
    */
    public void StartConsuming(Func<string, string> sendCommand)
    {
        var semaphore = new Semaphore(0, 1);
        LinkedListNode<QueuedCommand>? lastCommandNode = null;
        while (true)
        {
            if (this.CommandQueue.First == null || (lastCommandNode != null && lastCommandNode?.Next == null))
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
            var cmd = lastCommandNode.Value;
            var res = sendCommand(cmd.ToString());
            if (cmd.Semaphore != null)
            {
                cmd.Semaphore.Release();
            }
            Console.WriteLine(res);
            // TODO: if is getack then we signal
        }
    }

    public int GetWaitingReplicas()
    {
        return this.SemaphoreQueue.Count;
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

class QueuedCommand
{
    required public string Cmd;
    public SemaphoredCounter? Semaphore;

    override public string ToString()
    {
        return this.Cmd;
    }
}

class SemaphoredCounter
{
    required public int count;
    required public int releaseAt;

    private readonly Semaphore Semaphore = new(0, 1);

    public void Release()
    {
        count++;
        if (count == releaseAt)
        {
            this.Semaphore.Release();
        }
    }

    public int WaitResolved(int timeout)
    {
        Timer? timer = null;
        if (timeout >= 0)
        {
            timer = new Timer((object threadIdArg) =>
            {
                this.Semaphore.Release();
                timer?.Dispose();
            }, null, timeout, 2000);
        }
        this.Semaphore.WaitOne();
        return this.count;
    }
}