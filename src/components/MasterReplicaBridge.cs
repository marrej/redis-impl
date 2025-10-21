// This class handles the async writing and governs
// all operations related to the master-replica communication
class MasterReplicaBridge
{
    public bool IsMaster = false;

    // Data set only if the node is Master;
    public string? ReplId;

    public Int128? ProcessedBytes;

    public void SetMaster()
    {
        this.IsMaster = true;
        this.ProcessedBytes = 0;
        // Extend by "repl" - so that the length is 40 per requirement
        this.ReplId = System.Guid.NewGuid().ToString()+"repl";
    }

    public string GetReplicaInfo()
    {
        var role = "role:" + (this.IsMaster ? "master" : "slave");
        var replId = "master_replid:" + this.ReplId;
        var replOffset = "master_repl_offset:" + this.ProcessedBytes.ToString();
        return role + "\n" + replId + "\n" + replOffset + "\n";
    }
}