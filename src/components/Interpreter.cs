using System.Buffers.Text;
using System.Text.Json;

namespace RedisImpl
{
    class CommandItem
    {
        public required string Command;

        public required List<string> Arguments;
    }
    class Interpreter
    {
        // Id of the current thread.
        required public int Id;
        required public Storage Storage;
        required public MasterReplicaBridge Bridge;

        // If set to true, starts the replication process on bridge after the original command is finished.
        public bool StartReplication = false;

        private List<CommandItem>? CommandQueue = null;

        // New replica connection that is being established.
        // The data is stored here only until handshake is reached. A new replconf would invalidate this.
        public ReplicaConn? NewReplica;
        public string Execute(List<string> p)
        {
            var command = p.Count > 0 ? p[0] : "ERROR";
            var arguments = p.Count > 1 ? p[1..] : [];
            // Enqueue commands if queue exists
            if (command.ToUpper() != "EXEC" && command.ToUpper() != "DISCARD" && CommandQueue != null)
            {
                this.CommandQueue.Add(new CommandItem { Command = command, Arguments = arguments });
                return Types.GetSimpleString("QUEUED");
            }

            // Invariant: directly executing commands
            return this.ExecCmd(command, arguments);
        }

        // Blocking commands should be changed to non blocking after being consumed
        static private HashSet<string> EditCommands = ["SET", "INCR", "RPUSH", "LPUSH", "RPOP", "LPOP", "BLPOP", "XADD"];

        private string ExecCmd(string command, List<string> arguments)
        {
            try
            {
                var ret = command.ToUpper() switch
                {
                    // Redis info
                    "INFO" => this.Info(arguments),
                    "REPLCONF" => this.Replconf(arguments),
                    "PSYNC" => this.Psync(arguments),
                    // Queue commands
                    "MULTI" => this.Multi(),
                    "EXEC" => this.Exec(),
                    "DISCARD" => this.Discard(),
                    // Debugging commands
                    "PING" => Types.GetSimpleString("PONG"),
                    "ECHO" => this.Echo(arguments),
                    "TYPE" => this.Type(arguments),
                    // Item actions
                    "GET" => this.Get(arguments),
                    "SET" => this.Set(arguments),
                    "INCR" => this.Incr(arguments),
                    // List actions
                    "RPUSH" => this.Rpush(arguments),
                    "LRANGE" => this.Lrange(arguments),
                    "LPUSH" => this.Lpush(arguments),
                    "LLEN" => this.Llen(arguments),
                    "LPOP" => this.Lpop(arguments),
                    "BLPOP" => this.Blpop(arguments),
                    // Stream actions
                    "XADD" => this.Xadd(arguments),
                    "XRANGE" => this.XRange(arguments),
                    "XREAD" => this.XRead(arguments),
                    _ => Types.GetSimpleString("ERROR no command specified"),
                };

                // Store message for replica consumption
                if (EditCommands.Contains(command.ToUpper()) && this.Bridge.IsMaster)
                {
                    this.Bridge.QueueCommand(command, arguments);
                }

                return ret;
            }
            catch (Exception e)
            {
                return Types.GetSimpleError(e.Message);
            }
        }

        public string Psync(List<string> arguments)
        {
            if (arguments.Count < 2)
            {
                return Types.GetSimpleError("ERROR invalid parameter count");
            }
            if (this.NewReplica == null)
            { 
                return Types.GetSimpleError("ERR replica not initialized");
            }
            this.NewReplica.MasterReplId = arguments[0];
            this.NewReplica.ConsumedBytes = Int128.Parse(arguments[1]);
            this.Bridge.AddReplica(this.NewReplica);
            this.StartReplication = true;
            return Types.GetSimpleString("FULLRESYNC " + this.Bridge.ReplId + " " + this.Bridge.ProducedBytes.ToString());
        }

        public string Replconf(List<string> arguments)
        {
            if (arguments.Count == 2 && arguments[0].ToUpper() == "GETACK")
            {
                // TODO: attach the current processed byte offset
                return Types.GetStringArray(["REPLCONF", "ACK", "0"]);
            }
            if (arguments.Count > 1 && arguments[0] == "listening-port")
            {
                this.NewReplica = new ReplicaConn { Port = Int32.Parse(arguments[1]) };
            }
            // Currently automatically responds happily everytime;
            // TODO: But should also set the port of the follower
            return Types.GetSimpleString("OK");
        }

        public string Info(List<string> arguments)
        {
            HashSet<string> selection = arguments.Count == 0 ? ["all"] : arguments.ToHashSet();

            List<string> infoKeyVals = [];
            foreach (var s in selection)
            {
                switch (s)
                {
                    case "all":
                    case "replication":
                    default:
                        infoKeyVals.Add(this.Bridge.GetReplicaInfo());
                        break;
                }
            }
            return Types.GetBulkString(infoKeyVals);
        }

        public string Multi()
        {
            this.CommandQueue = [];
            return Types.GetSimpleString("OK");
        }

        public string Discard()
        {
            if (this.CommandQueue == null)
            {
                return Types.GetSimpleError("ERR DISCARD without MULTI");
            }
            this.CommandQueue = null;
            return Types.GetSimpleString("OK");
        }

        public string Exec()
        {
            if (this.CommandQueue == null)
            {
                throw new Exception("ERR EXEC without MULTI");
            }
            List<string> results = [];
            foreach (var cmd in this.CommandQueue)
            {
                results.Add(this.ExecCmd(cmd.Command, cmd.Arguments));
            }
            this.CommandQueue = null;

            return Types.GetStringArray(results, /*useEncodedString=*/ true);
        }

        public string Echo(List<string> arguments)
        {
            return Types.GetBulkString(arguments);
        }

        // https://redis.io/docs/latest/commands/incr
        public string Incr(List<string> arguments)
        {
            if (arguments.Count < 1)
            {
                return Types.GetSimpleError("ERROR invalid parameter count");
            }
            var key = arguments[0];
            return Types.GetInteger64(this.Storage.Incr(key));
        }


        // https://redis.io/docs/latest/commands/set
        public string Set(List<string> arguments)
        {
            if (arguments.Count < 2)
            {
                return Types.GetSimpleError("ERROR invalid parameter count");
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
                            return Types.GetSimpleError("ERROR ttl already set");
                        }
                        measurement = command;
                        if (arguments.Count < i + 1)
                        {
                            return Types.GetSimpleError("ERROR missing time variable");
                        }
                        time = int.Parse(arguments[i + 1]);
                        i++;
                        break;
                    case "KEEPTTL":
                        if (measurement != null)
                        {
                            return Types.GetSimpleError("ERROR ttl already set");
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
                throw new Exception("ERROR setting val");
            }
        }

        public string Get(List<string> arguments)
        {
            if (arguments.Count < 1)
            {
                return Types.GetSimpleError("ERROR missing key");
            }

            var val = this.Storage.Get(arguments[0]);
            return Types.GetBulkString(val == null ? null : [val]);
        }

        public string Rpush(List<string> arguments)
        {
            if (arguments.Count < 2)
            {
                return Types.GetSimpleError("ERROR invalida arg count");
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
                return Types.GetSimpleError("ERROR incorrect argument count");
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
                return Types.GetSimpleError("ERROR incorrect argument count");
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
                return Types.GetSimpleError("ERROR incorrect argument count");
            }
            return Types.GetInteger(Storage.Llen(arguments[0]));
        }

        // https://redis.io/docs/latest/commands/blpop/
        public string Blpop(List<string> arguments)
        {
            if (arguments.Count < 2)
            {
                return Types.GetSimpleError("ERROR incorrect argument count");
            }
            // BLPOP key [key ...] timeout
            var lists = arguments[0..(arguments.Count - 1)];
            var timeout = Double.Parse(arguments[^1]);
            var ret = this.Storage.Blpop(lists, this.Id, timeout);

            return Types.GetStringArray(ret);
        }

        public string Lpop(List<string> arguments)
        {
            if (arguments.Count < 1)
            {
                return Types.GetSimpleError("ERROR incorrect argument count");
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

        public string Type(List<string> arguments)
        {
            if (arguments.Count != 1)
            {
                return Types.GetSimpleError("ERROR incorrect argument count");
            }
            var property = arguments[0];

            // TODO: add other field retrievals (e.g. from list, currently retrieves only from the single kay val)
            // Accesses directly storage to avoid the formatiing abstractions
            var val = this.Storage.Get(property);
            if (val != null)
            {
                return Types.GetSimpleString("string");
            }

            var hasList = this.Storage.HasList(property);
            if (hasList)
            {
                return Types.GetSimpleString("list");
            }

            var hasStream = this.Storage.HasStream(property);
            if (hasStream)
            {
                return Types.GetSimpleString("stream");
            }

            return Types.GetSimpleString("none");
        }

        // https://redis.io/docs/latest/commands/xadd/
        public string Xadd(List<string> arguments)
        {
            if (arguments.Count < 4)
            {
                return Types.GetSimpleError("ERROR incorrect argument count");
            }
            var streamName = arguments[0];
            // We are currently ignoring all modifiers
            var id = arguments[1];

            List<KeyValPair> kvs = [];
            for (var i = 2; i < arguments.Count; i += 2)
            {
                // Should we ignore values that don't exist?
                if (i + 1 < arguments.Count - 1)
                {
                    break;
                }
                kvs.Add(new KeyValPair { Key = arguments[i], Val = arguments[i + 1] });
            }
            var item = new StreamItem { Id = id, KVs = kvs, Time = 0, SerieId = 0 };
            return Types.GetBulkString([this.Storage.Xadd(streamName, item)]);
        }

        // https://redis.io/docs/latest/commands/xrange/
        public string XRange(List<string> arguments)
        {
            if (arguments.Count != 3)
            {
                return Types.GetSimpleError("ERR incorrect param count");
            }
            var stream = arguments[0];
            var start = arguments[1];
            var end = arguments[2];
            return Types.GetArray(this.Storage.Xrange(stream, start, end));
        }

        // https://redis.io/docs/latest/commands/xread
        public string XRead(List<string> arguments)
        {
            if ((arguments.Count % 2) != 1)
            {
                return Types.GetSimpleError("ERR incorrect param count");
            }
            int blockMs = -1;
            bool hasBlockMs = arguments[0] == "block";
            if (hasBlockMs)
            {
                blockMs = Int32.Parse(arguments[1]);
            }
            var startArg = hasBlockMs ? 2 : 0;
            // Ignores the "streams" keyword which is used as separator
            if (arguments[startArg] != "streams")
            {
                return Types.GetSimpleError("ERR missing STREAMS param");
            }
            // Shift the start & midpoint if there is block param.
            var startOffset = startArg + 1;
            var midpoint = ((arguments.Count - startOffset) / 2) + startOffset;

            var streams = arguments[startOffset..midpoint].ToArray();
            var starts = arguments[midpoint..].ToArray();
            return Types.GetArray(this.Storage.Xread(streams, starts, blockMs));
        }
    }
}