using NetMQ;
using NetMQ.Sockets;
using MessagePack;

Console.WriteLine("Servidor de Mensagens (C# / MessagePack) iniciado...");
string logFile = "/data/messages.log";

using (var repSocket = new ResponseSocket("@tcp://*:5557")) 
using (var pubSocket = new PublisherSocket("@tcp://*:5556")) 
{
    Console.WriteLine("Socket REP (comandos) escutando na porta 5557.");
    Console.WriteLine("Socket PUB (broadcast) escutando na porta 5556.");
    Console.WriteLine($"Log de persistência em: {logFile}");

    var options = MessagePackSerializerOptions.Standard.WithCompression(MessagePackCompression.Lz4Block);

    using (var poller = new NetMQPoller { repSocket })
    {
        repSocket.ReceiveReady += (s, a) =>
        {
            byte[] commandBytes = repSocket.ReceiveFrameBytes();
            
            Dictionary<string, string> commandData;
            try
            {
                commandData = MessagePackSerializer.Deserialize<Dictionary<string, string>>(commandBytes, options);
                Console.WriteLine($"Comando recebido: {commandData["command"]}:{commandData["topic"]}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao desserializar comando: {ex.Message}");
                repSocket.SendFrame("ERROR: Formato inválido.");
                return;
            }

            try
            {
                string logEntry = $"{DateTime.UtcNow:O} | {commandData["command"]}:{commandData["topic"]}:{commandData["payload"]}\n";
                File.AppendAllText(logFile, logEntry);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao persistir log: {ex.Message}");
            }

            string command = commandData["command"];
            string topic = commandData["topic"];
            string payload = commandData["payload"];

            if (command == "publish" || command == "private")
            {
                string broadcastMessage = $"[{topic}] {payload}";
                
                var broadcastData = new Dictionary<string, string>
                {
                    { "message", broadcastMessage }
                };
                byte[] broadcastBytes = MessagePackSerializer.Serialize(broadcastData, options);

                Console.WriteLine($"Transmitindo no tópico '{topic}'");
                
                pubSocket.SendMoreFrame(topic)
                         .SendFrame(broadcastBytes); 
                
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