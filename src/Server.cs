using System.Data.SqlTypes;
using System.Net;
using System.Net.Sockets;
using System.Text;
using System.Text.Json;
using System.Threading;
using RedisImpl;

// We can test the connection using netcat, e.g. `echo "foo" | nc localhost 6379`
// Or better `echo -e "PING\nPING" | redis-cli`

Console.WriteLine("Server Starting");

TcpListener server = new(IPAddress.Any, 6379);
server.Start();
Storage storage = new();
Parser parser = new();
Console.WriteLine("Accepting at 6379");

// A communication loop listenting to socket exchanges
var id = 0;
while (true)
{
    Socket socket = server.AcceptSocket(); // wait for client
    Console.WriteLine("Client connected"); // Accepts a socket to connect
    // Resolves all commands sent from the socket in succession

    Thread conn = new Thread(ThreadLoop);
    conn.Start(new CliInput { Socket = socket, Id = id, Parser = parser, Storage = storage });
    id++;
}

void ThreadLoop(object data)
{
    CliInput c = (CliInput)data;
    Console.WriteLine("Connected thread {0}", c.Id);
    Interpreter interpreter = new() { Storage = c.Storage, Id = c.Id };

    while (true)
    {
        byte[] buffer = new byte[1024];
        var bytes = c.Socket.Receive(buffer);
        // Disconnect if no more requests are received (the client errors out)
        if (bytes == 0)
        {
            break;
        }
        var message = Encoding.ASCII.GetString(buffer);
        try
        {
            var p = c.Parser.Parse(message);
            var i = interpreter.Interpret(p);

            // Responds using SimpleString which is. "+`${ret}`\r\n" at all times
            var response = Encoding.ASCII.GetBytes(i);
            c.Socket.Send(response);
            Console.WriteLine("Response Sent");
        }
        catch (Exception e)
        {
            var r = Encoding.ASCII.GetBytes(Types.GetSimpleString("ERROR didn't parse correctly"));
            c.Socket.Send(r);
            continue;
        }
    }
}

class CliInput
{
    required public int Id { get; set; }
    required public Socket Socket { get; set; }

    required public Parser Parser { get; set; }
    required public Storage Storage { get; set; }
}

