namespace RedisImpl
{

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
        private void EvaluateTTL(string key)
        {
            if (TTLs.ContainsKey(key) && TTLs[key] < DateTime.Now)
            {
                TTLs.Remove(key);
                S.Remove(key);
            }
        }

        // TODO: make async so that the update runs on a new thread and doesn't blockt he push caller.
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
    }
}