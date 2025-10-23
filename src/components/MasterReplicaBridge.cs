// This class handles the async writing and governs
// all operations related to the master-replica communication
using System.Net.Sockets;
using System.Text;
using RedisImpl;

class MasterReplicaBridge
{
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

    public void SetRole(MasterInfo? info)
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
            this.ConnectToMaster(info);
        }
    }

    private void ConnectToMaster(MasterInfo info)
    {
        Console.WriteLine("Connecting to master");
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
        foreach (var r in reqs)
        {
            byte[] sendBuffer = Encoding.UTF8.GetBytes(Types.GetStringArray(r));
            stream.Write(sendBuffer);

            byte[] buffer = new byte[1024];
            stream.Socket.Receive(buffer);
            var message = Encoding.ASCII.GetString(buffer);

            Console.WriteLine(message);
            if (r[0] != "PSYNC" && !message.StartsWith("+FULLRESYNC"))
            {
                continue;
            }
            // TODO: parse the resync response
            // TODO: at this point stop the connection and wait for the Master to connect and to send data ourway.
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