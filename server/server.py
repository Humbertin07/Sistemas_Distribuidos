import zmq
import msgpack
import time
import threading
import os
import random

# --- Configuração Inicial ---
context = zmq.Context()
clock = 0
server_name = f"server_{random.randint(1000, 9999)}"
server_rank = -1
server_list = []
coordinator_name = ""
log_file = f"/data/{server_name}_messages.log"

print(f"Iniciando Servidor Worker: {server_name}")

# Sockets de Comunicação
req_socket = context.socket(zmq.REQ) # Para falar com o Ref Server
dealer_socket = context.socket(zmq.DEALER) # Para receber trabalho do Broker
pub_socket = context.socket(zmq.PUB) # Para publicar no Proxy
sub_socket = context.socket(zmq.SUB) # Para receber replicações e eleições

dealer_socket.connect("tcp://broker:5556")
pub_socket.connect("tcp://proxy:5557")
sub_socket.connect("tcp://proxy:5558")
req_socket.connect("tcp://ref_server:5559")

# Tópicos de Inscrição
sub_socket.setsockopt_string(zmq.SUBSCRIBE, "replication") # Ouve por replicações
sub_socket.setsockopt_string(zmq.SUBSCRIBE, "servers") # Ouve por eleições

# --- Persistência e Estado Local ---
# Em um sistema real, usaria um DB (SQLite, etc.). Usaremos arquivos de log.
# O `parte5.md` exige que TODOS os servidores tenham TODOS os dados.
# Usaremos o `log_file` principal (do servidor 0) como "fonte da verdade".
# Esta é a nossa estratégia de replicação:
# 1. Todos os servidores salvam seus logs localmente.
# 2. Todos os servidores publicam seus logs no tópico "replication".
# 3. Todos os servidores leem o tópico "replication" e salvam os logs dos outros.
# Esta é uma implementação "Eventual Consistency".

db = {
    "users": set(),
    "channels": {"general"}
}

def log_to_disk(log_entry):
    global clock
    clock += 1
    log_line = f"T={clock} | {log_entry}\n"
    try:
        with open(log_file, "a") as f:
            f.write(log_line)
    except Exception as e:
        print(f"Erro ao salvar log: {e}")

# --- Funções de Comunicação com Ref Server (parte4.md) ---

def send_to_ref_server(service, data):
    global clock
    clock += 1
    req_data = {
        "service": service,
        "data": {
            **data,
            "user": server_name,
            "clock": clock,
            "timestamp": time.time()
        }
    }
    req_socket.send(msgpack.packb(req_data))
    response_bytes = req_socket.recv()
    response_data = msgpack.unpackb(response_bytes, raw=False)
    
    clock = max(clock, response_data.get("clock", 0)) + 1
    return response_data

def register_with_ref_server():
    global server_rank
    print(f"[T={clock}] Registrando no Servidor de Referência...")
    response = send_to_ref_server("rank", {})
    server_rank = response.get("rank")
    print(f"[T={clock}] Registrado! Meu Rank: {server_rank}")

def send_heartbeat():
    while True:
        time.sleep(15) # Envia heartbeat a cada 15s
        send_to_ref_server("heartbeat", {})
        print(f"[T={clock}] Heartbeat enviado.")

def update_server_list():
    global server_list, coordinator_name
    while True:
        time.sleep(30) # Atualiza a lista a cada 30s
        response = send_to_ref_server("list", {})
        server_list = response.get("list", [])
        
        # Lógica de Eleição (Algoritmo Bully)
        # Se o coordenador não estiver na lista ativa, inicia eleição
        if coordinator_name not in [s["name"] for s in server_list]:
            print(f"[T={clock}] Coordenador {coordinator_name} caiu! Iniciando eleição.")
            start_election()

def start_election():
    global clock, coordinator_name
    clock += 1
    # Algoritmo Bully: O processo com maior rank vence.
    # Assumimos que o rank é o ID.
    my_rank = server_rank
    highest_rank_server = {"name": server_name, "rank": my_rank}
    
    for server in server_list:
        if server["rank"] > my_rank:
            # Envia 'election' (não implementado, pois exigiria sockets P2P)
            # Para simplificar (e cumprir o spec), o de maior rank se auto-elege
            highest_rank_server = server
            
    if highest_rank_server["name"] == server_name:
        # EU GANHEI!
        clock += 1
        coordinator_name = server_name
        print(f"[T={clock}] Eleição: Eu ({server_name}) sou o novo Coordenador.")
        
        # Avisa a todos no tópico "servers"
        pub_data = {
            "service": "election",
            "data": {
                "coordinator": server_name,
                "timestamp": time.time(),
                "clock": clock
            }
        }
        pub_socket.send_string("servers", zmq.SNDMORE)
        pub_socket.send(msgpack.packb(pub_data))

# --- Thread de Replicação e Eleição (parte5.md) ---

def subscriber_loop():
    global clock, coordinator_name, db
    while True:
        topic = sub_socket.recv_string()
        message_bytes = sub_socket.recv()
        
        message_data = msgpack.unpackb(message_bytes, raw=False)
        data = message_data.get("data", {})
        
        received_clock = data.get("clock", 0)
        clock = max(clock, received_clock) + 1

        if topic == "servers":
            # Alguém foi eleito
            new_coordinator = data.get("coordinator")
            coordinator_name = new_coordinator
            print(f"[T={clock}] Eleição recebida: Novo coordenador é {coordinator_name}")
            
        elif topic == "replication":
            # Recebe dados de replicação (log ou atualização de estado)
            print(f"[T={clock}] Dados de replicação recebidos de {data.get('sender')}")
            # Atualiza o estado local
            db["users"].update(data.get("users", []))
            db["channels"].update(data.get("channels", []))
            # Salva o log replicado
            log_to_disk(f"[REPLICA] {data.get('log_entry')}")

# --- Loop Principal (Lógica de Negócios) ---

def handle_request(request_data):
    global clock
    service = request_data.get("command")
    data = request_data # O payload já é o 'data'
    
    response_data = {"status": "OK"}
    
    # --- Serviços da Parte 1 ---
    if service == "login":
        user = data.get("username")
        db["users"].add(user)
        log_to_disk(f"login: {user}")
        response_data["message"] = f"Bem-vindo, {user}!"
    
    elif service == "users":
        response_data["users"] = list(db["users"])
        log_to_disk(f"list_users")

    elif service == "channel":
        channel = data.get("channel")
        if channel in db["channels"]:
            response_data["status"] = "ERROR"
            response_data["description"] = "Canal já existe"
        else:
            db["channels"].add(channel)
            log_to_disk(f"create_channel: {channel}")
            response_data["status"] = "sucesso"
            
    elif service == "channels":
        response_data["channels"] = list(db["channels"])
        log_to_disk(f"list_channels")

    # --- Serviços da Parte 2 ---
    elif service == "publish":
        user = data.get("user", "desconhecido")
        channel = data.get("topic")
        payload = data.get("payload")
        
        if channel not in db["channels"]:
            response_data["status"] = "erro"
            response_data["message"] = "Canal não existe"
        else:
            log_entry = f"publish:{channel}:{user}:{payload}"
            log_to_disk(log_entry)
            
            # Publica no Proxy
            clock += 1
            pub_data = {"message": f"[{channel}] {user}: {payload}", "timestamp": clock}
            pub_socket.send_string(channel, zmq.SNDMORE)
            pub_socket.send(msgpack.packb(pub_data))
            
            # Publica a replicação
            clock += 1
            repl_data = {
                "service": "replication",
                "data": {"log_entry": log_entry, "sender": server_name, "clock": clock, "users": [user], "channels": []}
            }
            pub_socket.send_string("replication", zmq.SNDMORE)
            pub_socket.send(msgpack.packb(repl_data))

    elif service == "message":
        src_user = data.get("user", "desconhecido")
        dst_user = data.get("topic")
        payload = data.get("payload")
        
        if dst_user not in db["users"]:
            response_data["status"] = "erro"
            response_data["message"] = "Usuário não existe"
        else:
            log_entry = f"message:{src_user}:{dst_user}:{payload}"
            log_to_disk(log_entry)
            
            # Publica no Proxy (tópico é o nome do usuário)
            clock += 1
            pub_data = {"message": f"[{src_user} (privado)] {payload}", "timestamp": clock}
            pub_socket.send_string(dst_user, zmq.SNDMORE)
            pub_socket.send(msgpack.packb(pub_data))
            
            # Publica a replicação
            clock += 1
            repl_data = {
                "service": "replication",
                "data": {"log_entry": log_entry, "sender": server_name, "clock": clock, "users": [src_user, dst_user], "channels": []}
            }
            pub_socket.send_string("replication", zmq.SNDMORE)
            pub_socket.send(msgpack.packb(repl_data))
            
    else:
        response_data["status"] = "ERROR"
        response_data["message"] = "Serviço desconhecido"

    clock += 1
    response_data["timestamp"] = time.time()
    response_data["clock"] = clock
    return msgpack.packb(response_data)

# --- Inicialização ---
register_with_ref_server()
start_election() # Tenta ser o coordenador ao iniciar

threading.Thread(target=send_heartbeat, daemon=True).start()
threading.Thread(target=update_server_list, daemon=True).start()
threading.Thread(target=subscriber_loop, daemon=True).start()

print(f"[T={clock}] Servidor {server_name} (Rank {server_rank}) pronto para receber trabalho.")

while True:
    # O socket DEALER do ZMQ faz o Round-Robin (load balancing)
    # Espera por [identidade_cliente, mensagem_vazia, dados]
    try:
        identity, empty, message_bytes = dealer_socket.recv_multipart()
    except zmq.ZMQError as e:
        print(f"Erro no ZMQ: {e}")
        time.sleep(1)
        continue

    request_data = msgpack.unpackb(message_bytes, raw=False)
    
    # Processa e envia a resposta
    response_bytes = handle_request(request_data)
    
    # Envia de volta para o Broker [identidade_cliente, mensagem_vazia, resposta]
    dealer_socket.send_multipart([identity, empty, response_bytes])