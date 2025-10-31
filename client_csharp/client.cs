using NetMQ;
using NetMQ.Sockets;
using MessagePack;
using MessagePack.Resolvers;
using System.Threading;

public class ClientCsharp
{
    private static long _clock = 0;
    private static readonly string USERNAME = "csharp_user_1";
    private static readonly MessagePackSerializerOptions _options = MessagePackSerializerOptions.Standard
        .WithResolver(ContractlessStandardResolver.Instance);
    
    // Trava para sincronizar o relógio entre as threads
    private static readonly object _clockLock = new object();

    private static long GetClock()
    {
        lock (_clockLock)
        {
            return _clock;
        }
    }

    private static void UpdateClock(long receivedClock)
    {
        lock (_clockLock)
        {
            _clock = Math.Max(_clock, receivedClock) + 1;
        }
    }
    
    private static long IncrementClock()
    {
        lock (_clockLock)
        {
            _clock++;
            return _clock;
        }
    }

    // Thread do Subscriber
    public static void SubscriberThread()
    {
        Console.WriteLine($"[SUB] Conectando ao Proxy em tcp://proxy:5558");
        using (var subSocket = new SubscriberSocket())
        {
            subSocket.Connect("tcp://proxy:5558");
            subSocket.Subscribe(USERNAME);
            subSocket.Subscribe("general");

            while (true)
            {
                var topic = subSocket.ReceiveFrameString();
                var messageBytes = subSocket.ReceiveFrameBytes();
                var messageData = MessagePackSerializer.Deserialize<Dictionary<string, object>>(messageBytes, _options);

                var receivedTimestamp = Convert.ToInt64(messageData["timestamp"]);
                UpdateClock(receivedTimestamp);

                Console.Write($"\r{(new string(' ', 80))}\r"); // Limpa a linha
                Console.WriteLine($"(T={GetClock()}) {messageData["message"]}");
                Console.Write("> ");
            }
        }
    }

    // Função Principal
    public static async Task Main(string[] args)
    {
        Console.WriteLine("Cliente C# (Arquitetura Final) iniciado...");
        
        using (var reqSocket = new RequestSocket())
        {
            reqSocket.Connect("tcp://broker:5555");

            // Autenticação
            var authResponse = await SendRequest(reqSocket, new Dictionary<string, object>
            {
                { "command", "login" },
                { "username", USERNAME }
            });
            Console.WriteLine($"[T={GetClock()}] Resposta da autenticação: '{authResponse["status"]}'");
            if ((string)authResponse["status"] != "OK") return;

            // Inicia o Subscriber
            var subThread = new Thread(SubscriberThread);
            subThread.IsBackground = true;
            subThread.Start();
            
            Console.WriteLine("\nConectado! Comandos:");
            Console.WriteLine("/publicar <mensagem>");
            Console.WriteLine("/msg <usuario> <mensagem>");
            Console.WriteLine("/usuarios");
            Console.WriteLine("/canais");
            Console.WriteLine("/criar <canal>");
            Console.WriteLine("/sair");

            while (true)
            {
                Console.Write("> ");
                string line = Console.ReadLine();
                if (string.IsNullOrWhiteSpace(line)) continue;
                if (line.ToLower() == "/sair") break;

                var commandData = new Dictionary<string, object>();

                if (line.StartsWith("/usuarios"))
                {
                    commandData["command"] = "users";
                    var res = await SendRequest(reqSocket, commandData);
                    Console.WriteLine($"Usuários: {MessagePackSerializer.SerializeToJson(res["users"])}");
                }
                else if (line.StartsWith("/canais"))
                {
                    commandData["command"] = "channels";
                    var res = await SendRequest(reqSocket, commandData);
                    Console.WriteLine($"Canais: {MessagePackSerializer.SerializeToJson(res["channels"])}");
                }
                else if (line.StartsWith("/criar "))
                {
                    commandData["command"] = "channel";
                    commandData["channel"] = line.Split(" ", 2)[1];
                    var res = await SendRequest(reqSocket, commandData);
                    Console.WriteLine($"Criar canal: {res["status"]}");
                }
                else if (line.StartsWith("/msg "))
                {
                    var parts = line.Split(" ", 3);
                    commandData["command"] = "message";
                    commandData["user"] = USERNAME;
                    commandData["topic"] = parts[1];
                    commandData["payload"] = $"{USERNAME} (privado): {parts[2]}";
                    var res = await SendRequest(reqSocket, commandData);
                    if ((string)res["status"] != "OK") Console.WriteLine($"Erro: {res["message"]}");
                }
                else
                {
                    commandData["command"] = "publish";
                    commandData["user"] = USERNAME;
                    commandData["topic"] = "general";
                    commandData["payload"] = $"{USERNAME}: {line}";
                    var res = await SendRequest(reqSocket, commandData);
                    if ((string)res["status"] != "OK") Console.WriteLine($"Erro: {res["message"]}");
                }
            }
        }
    }

    public static async Task<Dictionary<string, object>> SendRequest(RequestSocket socket, Dictionary<string, object> data)
    {
        data["timestamp"] = IncrementClock();
        var bytes = MessagePackSerializer.Serialize(data, _options);
        
        socket.SendFrame(bytes);
        
        var responseBytes = await socket.ReceiveFrameBytesAsync();
        var responseData = MessagePackSerializer.Deserialize<Dictionary<string, object>>(responseBytes.AsMemory, _options);
        
        UpdateClock(Convert.ToInt64(responseData["clock"]));
        return responseData;
    }
}