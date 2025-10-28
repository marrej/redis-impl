using System.Text.Json;

namespace RedisImpl
{

    class Parser
    {
        // Parses the input in to RDB and multiple commands
        public List<List<string>>? ParseCommandLists(string input)
        {
            var lines = input.Split("\r\n");
            var isRdbLenght = input[0] == '$';
            var isPureRDB = input.StartsWith("REDIS");
            if (isRdbLenght || isPureRDB)
            {
                var rdbSkip = 0;
                // Parsing RDB
                if (isRdbLenght)
                {
                    var rdbLen = Int32.Parse(lines[0][1..]);
                    rdbSkip = lines[0].Length + "\r\n".Length + rdbLen;
                }
                else
                {
                    rdbSkip = lines[0].Length;
                }
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
    }
}