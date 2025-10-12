namespace RedisImpl
{
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
                "TYPE" => this.Type(arguments),
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
                // Stream actions
                "XADD" => this.Xadd(arguments),
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

            var val = this.Storage.Get(arguments[0]);
            if (val == null)
            {
                return Types.GetBulkString(null);
            }
            return Types.GetBulkString([val]);
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
            var timeout = Double.Parse(arguments[^1]);
            var ret = this.Storage.Blpop(lists, this.Id, timeout);

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

        public string Type(List<string> arguments)
        {
            if (arguments.Count != 1)
            {
                return Types.GetSimpleString("ERROR incorrect argument count");
            }
            var property = arguments[0];

            // TODO: add other field retrievals (e.g. from list, currently retrieves only from the single kay val)
            // Accesses directly storage to avoid the formatiing abstractions
            Console.WriteLine("Access");
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
                return Types.GetSimpleString("ERROR incorrect argument count");
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
            var item = new StreamItem { Id = id, KVs = kvs };
            return Types.GetBulkString([this.Storage.Xadd(streamName, item)]);
        }
    }
}