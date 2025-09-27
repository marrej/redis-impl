using System.Net;
using System.Net.Sockets;
using System.Text;

// We can test the connection using netcat, e.g. `echo "foo" | nc localhost 6379`
// Or better `echo -e "PING\nPING" | redis-cli`

Console.WriteLine("Server Starting");

TcpListener server = new TcpListener(IPAddress.Any, 6379);
server.Start();
Console.WriteLine("Accepting at 6379");

// A communication loop listenting to socket exchanges

Socket socket = server.AcceptSocket(); // wait for client
Console.WriteLine("Client connected"); // Accepts a socket to connect
// Resolves all commands sent from the socket in succession
while (true)
{
    byte[] buffer = new byte[1024]; 
    var bytes = socket.Receive(buffer);
    var message = Encoding.ASCII.GetString(buffer);
    Console.WriteLine(message);

    // Currently responds to all commands as PONG
    // Responds using SimpleString which is. "+`${ret}`\r\n" at all times
    var response = Encoding.ASCII.GetBytes("+PONG\r\n");
    socket.Send(response);
    Console.WriteLine("Response Sent");
}