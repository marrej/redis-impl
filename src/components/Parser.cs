namespace RedisImpl
{

    class Parser
    {
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