using NetMQ;
using NetMQ.Sockets;

Console.WriteLine("Servidor de Mensagens (C#) iniciado...");
string logFile = "/data/messages.log";

using (var repSocket = new ResponseSocket("@tcp://*:5557")) 
using (var pubSocket = new PublisherSocket("@tcp://*:5556")) 
{
    Console.WriteLine("Socket REP (comandos) escutando na porta 5557.");
    Console.WriteLine("Socket PUB (broadcast) escutando na porta 5556.");
    Console.WriteLine($"Log de persistência em: {logFile}");

    using (var poller = new NetMQPoller { repSocket })
    {
        repSocket.ReceiveReady += (s, a) =>
        {
            string messageFromClient = repSocket.ReceiveFrameString();
            Console.WriteLine($"Comando recebido: '{messageFromClient}'");

            try
            {
                File.AppendAllText(logFile, $"{DateTime.UtcNow:O} | {messageFromClient}\n");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao persistir log: {ex.Message}");
            }

            string[] parts = messageFromClient.Split(':', 3);
            if (parts.Length != 3)
            {
                repSocket.SendFrame("ERROR: Formato inválido. Use 'comando:topico:mensagem'.");
                return;
            }

            string command = parts[0];
            string topic = parts[1];   
            string message = parts[2];

            if (command == "publish" || command == "private")
            {
                string broadcastMessage = $"[{topic}] {message}";
                Console.WriteLine($"Transmitindo no tópico '{topic}': {broadcastMessage}");
                
                pubSocket.SendMoreFrame(topic)
                         .SendFrame(broadcastMessage); 
                
                repSocket.SendFrame("OK_ENVIADO");
            }
            else
            {
                repSocket.SendFrame("ERROR: Comando desconhecido.");
            }
        };

        poller.Run();
    }
}