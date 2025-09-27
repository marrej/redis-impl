using System.Net;
using System.Net.Sockets;
using System.Text;

// We can test the connection using netcat, e.g. `echo "foo" | nc localhost 6379`

Console.WriteLine("Server Starting");

TcpListener server = new TcpListener(IPAddress.Any, 6379);
server.Start();
Console.WriteLine("Accepting at 6379");
Socket socket = server.AcceptSocket(); // wait for client
Console.WriteLine("Client connected");

// Currently responds to all commands as PONG
// Responds using SimpleString which is. "+`${ret}`\r\n" at all times
var response = Encoding.ASCII.GetBytes("+PONG\r\n");
socket.Send(response);