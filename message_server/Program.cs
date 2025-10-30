using NetMQ;
using NetMQ.Sockets;
using MessagePack;
using MessagePack.Resolvers;

Console.WriteLine("Servidor de Mensagens (C# / Relógios Lamport) iniciado...");
string logFile = "/data/messages.log";
long clock = 0; 

using (var repSocket = new ResponseSocket("@tcp://*:5557")) 
using (var pubSocket = new PublisherSocket("@tcp://*:5556")) 
{
    Console.WriteLine("Socket REP (comandos) escutando na porta 5557.");
    Console.WriteLine("Socket PUB (broadcast) escutando na porta 5556.");
    Console.WriteLine($"Log de persistência em: {logFile}");

    var options = MessagePackSerializerOptions.Standard
        .WithResolver(ContractlessStandardResolver.Instance); 

    using (var poller = new NetMQPoller { repSocket })
    {
        repSocket.ReceiveReady += (s, a) =>
        {
            byte[] commandBytes = repSocket.ReceiveFrameBytes();
            
            Dictionary<string, object> commandData;
            try
            {
                commandData = MessagePackSerializer.Deserialize<Dictionary<string, object>>(commandBytes, options);
                
                long receivedTimestamp = Convert.ToInt64(commandData["timestamp"]);
                clock = Math.Max(clock, receivedTimestamp) + 1;
                
                Console.WriteLine($"[T={clock}] Comando recebido: {commandData["command"]}:{commandData["topic"]}");
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao desserializar comando: {ex.Message}");
                repSocket.SendFrame("ERROR: Formato inválido.");
                return;
            }

            try
            {
                string logEntry = $"{DateTime.UtcNow:O} | [T={clock}] | {commandData["command"]}:{commandData["topic"]}:{commandData["payload"]}\n";
                File.AppendAllText(logFile, logEntry);
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Erro ao persistir log: {ex.Message}");
            }

            string command = (string)commandData["command"];
            string topic = (string)commandData["topic"];
            string payload = (string)commandData["payload"];

            if (command == "publish" || command == "private")
            {
                string broadcastMessage = $"[{topic}] {payload}";
                
                clock++;
                
                var broadcastData = new Dictionary<string, object>
                {
                    { "message", broadcastMessage },
                    { "timestamp", clock } 
                };
                byte[] broadcastBytes = MessagePackSerializer.Serialize(broadcastData, options);

                Console.WriteLine($"[T={clock}] Transmitindo no tópico '{topic}'");
                
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