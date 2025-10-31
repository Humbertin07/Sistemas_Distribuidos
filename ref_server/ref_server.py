import zmq
import msgpack
import time

print("Iniciando Servidor de Referência...")
context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5559")

servers = {} # Armazena {nome: {rank, last_heartbeat}}
clock = 0
current_rank = 0

def get_timestamp():
    return time.time()

print("Servidor de Referência escutando na porta 5559.")

while True:
    message_bytes = socket.recv()
    request_data = msgpack.unpackb(message_bytes, raw=False)
    
    service = request_data.get("service")
    data = request_data.get("data", {})
    
    received_clock = data.get("clock", 0)
    clock = max(clock, received_clock) + 1
    
    response_data = {"clock": clock, "timestamp": get_timestamp()}
    
    if service == "rank":
        server_name = data.get("user")
        if server_name not in servers:
            servers[server_name] = {"rank": current_rank, "last_heartbeat": time.time()}
            current_rank += 1
        response_data["rank"] = servers[server_name]["rank"]
        print(f"[T={clock}] Servidor {server_name} registrado com rank {response_data['rank']}")
        
    elif service == "list":
        print(f"[T={clock}] Pedido de lista recebido.")
        active_servers = []
        cutoff = time.time() - 30 # 30 segundos de timeout
        for name, info in list(servers.items()):
            if info["last_heartbeat"] > cutoff:
                active_servers.append({"name": name, "rank": info["rank"]})
            else:
                print(f"Servidor {name} removido por inatividade.")
                del servers[name]
        response_data["list"] = active_servers
        
    elif service == "heartbeat":
        server_name = data.get("user")
        if server_name in servers:
            servers[server_name]["last_heartbeat"] = time.time()
            print(f"[T={clock}] Heartbeat recebido de {server_name}")
        else:
            response_data["status"] = "ERROR" # Servidor não registrado
            print(f"[T={clock}] Heartbeat de servidor desconhecido: {server_name}")

    clock += 1
    response_data["timestamp"] = get_timestamp()
    socket.send(msgpack.packb(response_data))