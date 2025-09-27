using System.Net;
using System.Net.Sockets;

Console.WriteLine("Server Starting");

TcpListener server = new TcpListener(IPAddress.Any, 6379);
server.Start();
Console.WriteLine("Accepting at 6379");
server.AcceptSocket(); // wait for client
Console.WriteLine("Finished");