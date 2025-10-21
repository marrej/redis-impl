// This class handles the async writing and governs
// all operations related to the master-replica communication
using System.Net.Sockets;
using System.Text;
using RedisImpl;

class MasterReplicaBridge
{
    public bool IsMaster = false;

    // Data set only if the node is Master;
    public string? ReplId;

    public Int128? ProcessedBytes;

    private MasterInfo? MasterConn;

    public void SetRole(MasterInfo? info)
    {
        if (info == null)
        {
            this.IsMaster = true;
            this.ProcessedBytes = 0;
            // Extend by "repl" - so that the length is 40 per requirement
            this.ReplId = System.Guid.NewGuid().ToString() + "repl";
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
        // Initiate connection - first just by sending Ping and then breaking the socket.
        while (true)
        {
            byte[] sendBuffer = Encoding.UTF8.GetBytes(Types.GetStringArray(["PING"]));
            stream.Write(sendBuffer);

            byte[] buffer = new byte[1024];
            stream.Socket.Receive(buffer);
            var message = Encoding.ASCII.GetString(buffer);
            Console.WriteLine(message);
            break;
        }
    }

    public string GetReplicaInfo()
    {
        var role = "role:" + (this.IsMaster ? "master" : "slave");
        var replId = "master_replid:" + this.ReplId;
        var replOffset = "master_repl_offset:" + this.ProcessedBytes.ToString();
        return role + "\n" + replId + "\n" + replOffset + "\n";
    }
}

class MasterInfo
{
    required public string Ip;
    required public int Port; 
}