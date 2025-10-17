using NetMQ;
using NetMQ.Sockets;

Console.WriteLine("Servidor de Mensagens (C#) iniciado...");

using (var repSocket = new ResponseSocket("@tcp://*:5557")) 
using (var pubSocket = new PublisherSocket("@tcp://*:5556")) 
{
    Console.WriteLine("Socket REP escutando na porta 5557.");
    Console.WriteLine("Socket PUB escutando na porta 5556.");

    using (var poller = new NetMQPoller { repSocket })
    {
        repSocket.ReceiveReady += (s, a) =>
        {
            string messageFromClient = repSocket.ReceiveFrameString();
            Console.WriteLine($"Recebido para publicar: '{messageFromClient}'");

            string topic = "general";
            string broadcastMessage = $"[{topic}] {messageFromClient}";

            Console.WriteLine($"Transmitindo no tópico '{topic}': {broadcastMessage}");
            pubSocket.SendMoreFrame(topic) 
                     .SendFrame(broadcastMessage); 

            repSocket.SendFrame("OK_PUBLICADO");
        };

        poller.Run();
    }
}