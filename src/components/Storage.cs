using System.Text.Json;

namespace RedisImpl
{
    class StreamItem
    {
        public required string Id;
        // Time and SerieId are derivatives of Id, but allow quicker comparison
        public required Int128 Time;
        public required Int64 SerieId;
        public required List<KeyValPair> KVs;

        public List<object> ToList()
        {
            List<string> kvList = [];
            foreach (var kv in KVs)
            {
                kvList.Add(kv.Key);
                kvList.Add(kv.Val);
            }
            return [
                Id,
                kvList
            ];
        }
    }

    class KeyValPair
    {
        public required string Key;
        public required string Val;
    }

    class WaitingTicket
    {
        required public int ThreadId;
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
    class Storage
    {
        // Stream only props
        readonly Dictionary<string, LinkedList<StreamItem>> Streams = [];
        readonly Dictionary<string, List<Semaphore>> StreamBlocks = [];

        // List Only props
        readonly Dictionary<string, LinkedList<string>> Lists = [];
        readonly Dictionary<string, BlockingQueue> BlockedListReaders = [];
        // Sync property checking whether there is Update list in progress to avoid multi triggers on parallel pushes.
        readonly HashSet<string> ListUpdatesInProgress = [];
        // Mutext used for popping data off the lists;
        readonly private Mutex BlockingPopMutex = new();

        // KeyVal Only props
        readonly Dictionary<string, string> KeyVals = [];
        readonly Dictionary<string, DateTime> TTLs = [];

        // Called on every interaction with the Storage to evaluate whether it shoudl evacuate a property.
        // Running on every call is costly. Might be better to evaluate just the key on call
        private void EvaluateTTL(string key)
        {
            if (TTLs.ContainsKey(key) && TTLs[key] < DateTime.Now)
            {
                TTLs.Remove(key);
                KeyVals.Remove(key);
            }
        }

        public bool HasStream(string name)
        {
            return Streams.ContainsKey(name);
        }

        public bool HasList(string name)
        {
            return Lists.ContainsKey(name);
        }

        // TODO: make async so that the update runs on a new thread and doesn't blockt he push caller.
        public void DidUpdateList(string list)
        {

            BlockedListReaders.TryGetValue(list, out BlockingQueue? bq);
            if (bq == null || bq?.Tickets.Count <= 0) { return; }

            Lists.TryGetValue(list, out LinkedList<string>? storage);
            if (storage == null || storage?.Count <= 0) { return; }

            if (ListUpdatesInProgress.Contains(list)) { return; }
            ListUpdatesInProgress.Add(list);
            while ((storage?.Count ?? 0) > 0)
            {
                var waiting = bq?.Tickets.First?.Value;
                var last = bq?.Tickets.Last?.Value;
                // Don't break on last, since the last could have timeouted early
                if (waiting == null || last == null) { break; }
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
                BlockedListReaders.Remove(list);
            }
            ListUpdatesInProgress.Remove(list);
        }

        public string? Get(string key)
        {
            this.EvaluateTTL(key);
            if (!KeyVals.TryGetValue(key, out string? value))
            {
                return null;
            }
            return value;
        }

        public void Set(string key, string val, SetOptions options)
        {
            this.EvaluateTTL(key);
            if (options == null)
            {
                KeyVals[key] = val;
                return;
            }

            var insertedValue = false;
            KeyVals.TryGetValue(key, out string? value);
            // Conditionally inserts the value. If not condition is set then inserts
            if (value != null && options.AddOnlyIfAlreadyDefined)
            {
                insertedValue = true;
                KeyVals[key] = val;
            }
            else if (value == null && options.AddOnlyIfNotDefined)
            {
                insertedValue = true;
                KeyVals[key] = val;
            }
            else if (!options.AddOnlyIfAlreadyDefined && !options.AddOnlyIfNotDefined)
            {
                insertedValue = true;
                KeyVals[key] = val;
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
            if (!Lists.ContainsKey(list))
            {
                Lists[list] = [];
            }
            Lists[list].AddLast(input);
            return Lists[list].Count;
        }

        public int Lpush(string list, List<string> items)
        {
            if (!Lists.ContainsKey(list))
            {
                Lists[list] = [];
            }

            foreach (var i in items)
            {
                Lists[list].AddFirst(i);
            }
            return Lists[list].Count;
        }

        public int Llen(string list)
        {
            Lists.TryGetValue(list, out LinkedList<string>? arr);
            return arr == null ? 0 : arr.Count;
        }

        // Doesn't error out but instead returns empty array
        public List<string> Lrange(string list, int start, int stop)
        {
            Lists.TryGetValue(list, out LinkedList<string>? arr);
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
            Lists.TryGetValue(list, out LinkedList<string>? arr);
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
        public List<string>? Blpop(List<string> lists, int id, double timeout)
        {
            // TODO: wrap the semaphore with an object containing also "timeout passed"
            Semaphore callBlocker = new(initialCount: 0, maximumCount: 1);
            var ticket = new WaitingTicket { ThreadId = id, Semaphore = callBlocker, Released = false };

            // Block the queue creation to avoid ticket dissapearance and racing between blocking calls.
            BlockingPopMutex.WaitOne();
            foreach (var l in lists)
            {
                // If others are waiting, then add ourselves to the lists and wait
                BlockedListReaders.TryGetValue(l, out BlockingQueue? blockingQueue);
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
                    BlockedListReaders.Add(l, bq);
                }
                bq.Tickets.AddLast(ticket);
            }

            BlockingPopMutex.ReleaseMutex();

            // Using direct timeout to signal the semaphore. SpinWaits might be effective in short term,
            // but if we wait for 5 secs or logner its totally unnecessary CPU hogging
            Timer? timer = null;
            if (timeout > 0)
            {
                timer = new Timer((object threadIdArg) =>
                {
                    // TODO (minor): note that this can cause the awaiting chain to grow without any cleanup.
                    // Maybe we should trigerr the cleanup here?
                    ticket.Released = true;
                    ticket.Semaphore.Release();
                    timer?.Dispose();
                }, null, Convert.ToInt32(1000 * timeout), 500);
            }

            // Wait for the ticket to be resolved
            callBlocker.WaitOne();

            if (ticket.List == null || ticket.Value == null)
            {
                return null;
            }
            return [ticket.List, ticket.Value];
        }

        // Allows user to either euto generate the whole Id via *
        // or just generate the next sessionId via TimeStamp-*
        // But unless * is used, new TS needs to be >, or the = with the SessionId increased.
        private string? ValidateAndGenerateXId(string name, StreamItem item)
        {
            if (item.Id == "0-0")
            {
                throw new Exception("ERR The ID specified in XADD must be greater than 0-0");
            }

            var id = item.Id;
            string[] timeStampAndSerieNumber = id.Split("-");
            bool generateAll = id == "*";
            if (!generateAll && timeStampAndSerieNumber.Length != 2)
            {
                return null;
            }

            Streams.TryGetValue(name, out LinkedList<StreamItem>? stream);
            var lastId = stream?.Last?.Value.Id;
            Int128? lastT = null;
            Int64? lastS = null;
            if (lastId != null)
            {
                var lastTimeAndSerie = lastId.Split("-");
                lastT = Int128.Parse(lastTimeAndSerie[0]);
                lastS = Int64.Parse(lastTimeAndSerie[1]);
            }

            if (generateAll)
            {
                if (lastT != null)
                {
                    return lastT + "-" + (lastS + 1).ToString();
                }
                else
                {
                    return DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString() + "-0";
                }
            }

            var T = Int128.Parse(timeStampAndSerieNumber[0]);
            var S = timeStampAndSerieNumber[1];
            if (lastT > T)
            {
                return null;
            }
            if (lastT == T && S != "*" && (Int64.Parse(S) <= lastS))
            {
                return null;
            }

            if (S == "*")
            {
                // If the Time is 0, then the serie number default is 1;
                int defaultSerieNumber = (T == 0 ? 1 : 0);
                return T + "-" + (lastS != null && lastT == T ? lastS + 1 : defaultSerieNumber).ToString();
            }
            return item.Id;
        }

        public string Xadd(string name, StreamItem item)
        {
            item.Id = this.ValidateAndGenerateXId(name, item) ?? throw new Exception("ERR The ID specified in XADD is equal or smaller than the target stream top item");
            var tAndS = item.Id.Split("-");
            if (tAndS.Length != 2)
            {
                throw new Exception("ERR incorrect id");
            }

            item.Time = Int128.Parse(tAndS[0]);
            item.SerieId = Int64.Parse(tAndS[1]);

            // If stream exists, we append, otherwise we create the stream.
            Streams.TryGetValue(name, out LinkedList<StreamItem>? stream);
            if (stream == null)
            {
                stream ??= new LinkedList<StreamItem>();
                Streams[name] = stream;
            }
            stream.AddLast(item);
            return item.Id;
        }

        private void AddStreamBlock(string n, Semaphore s)
        { 
            StreamBlocks.TryGetValue(n, out List<Semaphore>? blocks);
            if (blocks == null)
            {
                StreamBlocks[n] = [];
            }
            StreamBlocks[n].Add(s);
        }

        private void XreadBlock(string[] names, string[] starts, int blockMs)
        {
            Semaphore semaphore = new(initialCount: 0, maximumCount: 1);
            for (var i = 0; i < names.Length; i++)
            {
                var n = names[i];
                var s = starts[i];
                var (sTime, sSerie) = GetStreamStart(s);

                var stream = this.Streams[n];
                if (stream == null || stream.Count == 0)
                {
                    this.AddStreamBlock(n, semaphore);
                    continue;
                }

                var streamVal = stream?.Last?.Value;
                if (streamVal == null)
                {
                    this.AddStreamBlock(n, semaphore);
                    continue;
                }

                var vTime = streamVal.Time;
                var vSerie = streamVal.SerieId;
                if (vTime > sTime || (vTime == sTime && vSerie >= sSerie))
                {
                    // Don't add the semaphore in case there are already values
                    continue;
                }
                this.AddStreamBlock(n, semaphore);
            }
            
            Timer? timer = null;
            if (blockMs > 0)
            {
                timer = new Timer((object threadIdArg) =>
                {
                    // TODO (minor): note that this can cause the awaiting chain to grow without any cleanup.
                    // Maybe we should trigerr the cleanup here?
                    semaphore.Release();
                    timer?.Dispose();
                }, null, blockMs, 500);
            }

            semaphore.WaitOne();
        }

        public List<object>? Xread(string[] name, string[] start, int blockMs)
        {
            if (name.Length != start.Length)
            {
                throw new Exception("ERR Keys don't align with start ids");
            }

            if (blockMs > 0)
            {
                this.XreadBlock(name, start, blockMs);
            }

            List<object> streams = [];
            for (var i = 0; i < name.Length; i++)
            {
                var stream = this.Xrange(name[i], start[i], "+", /*startInclusive=*/false);
                if (stream == null)
                {
                    continue;
                }

                streams.Add(new List<object> { name[i], stream });
            }
            return streams;
        }

        private static (Int128 t, Int64 s) GetStreamStart(string start)
        {
            var ts = start == "-" ? ["0"] : start.Split("-");
            Int128 sTime = Int128.Parse(ts[0]);
            Int64 sSerie = ts.Length == 1 ? 0 : Int64.Parse(ts[1]);
            return (sTime, sSerie);
        }

        private static (Int128 t, Int64 s) GetStreamEnd(string end)
        {
            var te = end == "+" ? [Int64.MaxValue.ToString()] : end.Split("-");
            Int128 eTime = Int128.Parse(te[0]);
            Int64 eSerie = te.Length == 1 ? Int64.MaxValue : Int64.Parse(te[1]);
            return (eTime, eSerie);
        }

        public List<object>? Xrange(string name, string start, string end, bool startInclusive = true)
        {
            Streams.TryGetValue(name, out LinkedList<StreamItem>? stream);
            if (stream == null)
            {
                return null;
            }
            var (sTime, sSerie) = GetStreamStart(start);
            var (eTime, eSerie) = GetStreamEnd(end);

            List<object> ret = [];
            foreach (var v in stream)
            {
                var afterStart = v.Time > sTime;
                // startInclusive means that when the timestamp == start, then serieId needs to be bigger.
                var atTimeStart = v.Time == sTime && (startInclusive ? v.SerieId >= sSerie : v.SerieId > sSerie);

                var beforeEnd = v.Time < eTime;
                var atTimeEnd = v.Time == eTime && v.SerieId <= eSerie;
                if ((afterStart || atTimeStart) && (beforeEnd || atTimeEnd))
                {
                    ret.Add(v.ToList());
                }
            }
            return ret;
        }
    }
}