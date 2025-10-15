using System.Text.Json;

namespace RedisImpl
{
    class ErrorType
    { 
        public static string ERR = "ERR";
    }

    class Types
    {
        // https://redis.io/docs/latest/develop/reference/protocol-spec/#simple-errors
        public static string GetSimpleError(string message)
        {
            return "-" + message + "\r\n";
        }

        // https://redis.io/docs/latest/develop/reference/protocol-spec/#simple-strings
        public static string GetSimpleString(string i)
        {
            return "+" + i + "\r\n";
        }

        // https://redis.io/docs/latest/develop/reference/protocol-spec/#bulk-strings
        public static string GetBulkString(List<string>? inputs)
        {
            var bulkString = "$";
            if (inputs == null)
            {
                return bulkString + (-1).ToString() + "\r\n";
            }
            if (inputs.Count == 0)
            {
                return bulkString + "0\r\n\r\n";
            }
            foreach (var a in inputs)
            {
                var len = a.Length + "\r\n";
                var content = a + "\r\n";
                bulkString = bulkString + len + content;
            }
            return bulkString;
        }

        // https://redis.io/docs/latest/develop/reference/protocol-spec/#integers
        public static string GetInteger(int input)
        {
            return ":" + input.ToString() + "\r\n";
        }
        public static string GetInteger64(Int64 input)
        {
            return ":" + input.ToString() + "\r\n";
        }
        

        // https://redis.io/docs/latest/develop/reference/protocol-spec/#arrays
        public static string GetStringArray(List<string>? inputs)
        {
            if (inputs == null)
            {
                return "*-1\r\n";
            }
            var ret = "*" + inputs.Count.ToString() + "\r\n";
            for (var i = 0; i < inputs.Count; i++)
            {
                ret += Types.GetBulkString([inputs[i]]);
            }
            return ret;
        }

        public static string GetArray(List<object>? inputs)
        {
            if (inputs == null)
            {
                return "*-1\r\n";
            }
            var ret = "*" + inputs.Count.ToString() + "\r\n";
            for (var i = 0; i < inputs.Count; i++)
            {
                if (inputs[i] is string)
                {
                    ret += Types.GetBulkString([(string)inputs[i]]);
                }
                else if (inputs[i] is int)
                {
                    ret += Types.GetInteger((int)inputs[i]);
                }
                else if (inputs[i] is List<string>)
                {
                    ret += Types.GetStringArray((List<string>)inputs[i]);
                }
                else if (inputs[i] is List<int>)
                {
                    ret += Types.GetIntArray((List<int>)inputs[i]);
                }
                else if (inputs[i] is List<object>)
                {
                    ret += Types.GetArray((List<object>)inputs[i]);
                }
            }
            return ret;
        }

        public static string GetIntArray(List<int> inputs)
        {
            var ret = "*" + inputs.Count.ToString() + "\r\n";
            for (var i = 0; i < inputs.Count; i++)
            {
                ret += Types.GetInteger(inputs[i]);
            }
            return ret;
        }
    }
}