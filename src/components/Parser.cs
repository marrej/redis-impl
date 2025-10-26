using System.Text.Json;

namespace RedisImpl
{

    class Parser
    {
        public List<List<string>>? ParseCommandLists(string input)
        {
            Console.WriteLine("Processing Command Lists");
            Console.WriteLine(input);
            var lines = input.Split("\r\n");
            if (input[0] == '$')
            {
                // Parsing RDB
                var rdbLen = Int32.Parse(lines[0][1..]);
                var rdbSkip = lines[0].Length + "\r\n".Length + rdbLen;
                lines = input[rdbSkip..].Split("\r\n");
                // TODO: Process the RDB if any required
            }

            // Process all commands from the resto of the message
            List<List<string>> commands = [];
            for (var i = 0; i < lines.Length; i++)
            {
                // Skips empty line ends possibly attached to a message.
                if (lines[i].Replace("\u0000", "").Length == 0)
                {
                    continue;
                }
                int paramCount = Int32.Parse(lines[i][1..]);
                List<string> p = [];
                for (var j = 0; j < paramCount; j++)
                {
                    // Ignores the lines containeg the lenght o line counter
                    p.Add(lines[i + 2]);
                    i += 2;
                }
                commands.Add(p);
            }
            return commands;
        }
        public List<string>? Parse(string input)
        {
            Console.WriteLine(input);
            // The input contains
            // - *paramcount\r\n
            // - $length\r\n
            // - string\r\n
            var lines = input.Split("\r\n");
            try
            {
                if (lines[0][0] != '*')
                {
                    return null;
                }
                int paramCount = Int32.Parse(lines[0].Substring(1));
                List<string> p = [];
                for (var i = 0; i < paramCount; i++)
                {
                    // Ignores the lines containeg the lenght o line counter
                    p.Add(lines[2 + (i * 2)]);
                }
                return p;
            }
            catch (Exception e)
            {
                Console.WriteLine("Didn't parse input correctly");
                throw e;
            }
        }
    }
}