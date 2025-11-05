import zmq
import msgpack
import time
import os
import random
import threading

# Variáveis globais
context = zmq.Context()
clock = 0
coordinator_name = None
server_name = os.getenv("SERVER_NAME", f"server_{random.randint(1000, 9999)}")
db = {}
message_count = 0
req_socket_lock = threading.Lock()  # ← CORREÇÃO: Lock para thread safety

print(f"[Iniciando] Servidor: {server_name}")

# Sockets
pub_socket = context.socket(zmq.PUB)
pub_socket.connect("tcp://proxy:5557")

sub_socket = context.socket(zmq.SUB)
sub_socket.connect("tcp://proxy:5558")
sub_socket.setsockopt_string(zmq.SUBSCRIBE, "servers")
sub_socket.setsockopt_string(zmq.SUBSCRIBE, "replication")

req_socket = context.socket(zmq.REQ)
req_socket.connect("tcp://ref_server:5559")

# ← CORREÇÃO: Função para enviar ao ref_server com lock e timeout
def send_to_ref_server(service, data):
    global clock
    clock += 1
    req_data = {
        "service": service,
        "data": {**data, "user": server_name, "clock": clock, "timestamp": time.time()}
    }
    
    with req_socket_lock:  # Thread safety
        try:
            req_socket.send(msgpack.packb(req_data))
            req_socket.setsockopt(zmq.RCVTIMEO, 5000)  # 5 segundos timeout
            response = msgpack.unpackb(req_socket.recv(), raw=False)
            req_socket.setsockopt(zmq.RCVTIMEO, -1)  # Reset timeout
            clock = max(clock, response.get("clock", 0)) + 1
            return response
        except zmq.Again:
            print(f"[T={clock}] ⚠️  Timeout ao contatar ref_server")
            return {"status": "TIMEOUT"}
        except zmq.ZMQError as e:
            print(f"[T={clock}] ❌ Erro ZMQ: {e}")
            return {"status": "ERROR"}
        except Exception as e:
            print(f"[T={clock}] ❌ Erro: {e}")
            return {"status": "ERROR"}

# ← CORREÇÃO: Sincronização Berkeley melhorada
def synchronize_clock():
    global clock, coordinator_name
    
    if coordinator_name is None:
        print(f"[T={clock}] ⚠️  Sincronização cancelada: sem coordenador")
        return
    
    clock += 1
    print(f"[T={clock}] 🔄 Iniciando sincronização Berkeley...")
    
    # Solicitar tempo do coordenador
    request_msg = {
        "service": "clock_request",
        "data": {
            "requester": server_name,
            "clock": clock,
            "timestamp": time.time()
        }
    }
    
    pub_socket.send_string("servers", zmq.SNDMORE)
    pub_socket.send(msgpack.packb(request_msg))
    
    # Aguardar resposta com polling
    poller = zmq.Poller()
    poller.register(sub_socket, zmq.POLLIN)
    
    timeout = 3000  # 3 segundos
    start_time = time.time()
    
    while (time.time() - start_time) < (timeout / 1000):
        socks = dict(poller.poll(timeout=1000))
        
        if sub_socket in socks:
            try:
                topic = sub_socket.recv_string(zmq.NOBLOCK)
                message_bytes = sub_socket.recv(zmq.NOBLOCK)
                message_data = msgpack.unpackb(message_bytes, raw=False)
                
                if message_data.get("service") == "clock_response":
                    coordinator_time = message_data.get("data", {}).get("time")
                    if coordinator_time:
                        clock = max(clock, message_data.get("data", {}).get("clock", 0)) + 1
                        print(f"[T={clock}] ✅ Relógio sincronizado com coordenador")
                        return
            except zmq.Again:
                continue
            except Exception as e:
                print(f"[T={clock}] ⚠️  Erro na sincronização: {e}")
                continue
    
    print(f"[T={clock}] ⏱️  Timeout ao sincronizar com coordenador")

# Subscriber loop
def subscriber_loop():
    global clock, coordinator_name, db, message_count
    
    print(f"[T={clock}] 👂 Subscriber loop iniciado")
    
    while True:
        try:
            topic = sub_socket.recv_string()
            message_bytes = sub_socket.recv()
            message_data = msgpack.unpackb(message_bytes, raw=False)
            data = message_data.get("data", {})
            
            clock = max(clock, data.get("clock", 0)) + 1
            
            if topic == "servers":
                service = data.get("service")
                
                # Nova eleição de coordenador
                if data.get("coordinator"):
                    new_coord = data.get("coordinator")
                    if new_coord != coordinator_name:
                        coordinator_name = new_coord
                        print(f"[T={clock}] 🗳️  Novo coordenador: {coordinator_name}")
                
                # Pedido de sincronização de relógio
                elif service == "clock_request" and coordinator_name == server_name:
                    requester = data.get("requester")
                    if requester and requester != server_name:
                        clock += 1
                        response = {
                            "service": "clock_response",
                            "data": {
                                "time": time.time(),
                                "clock": clock,
                                "timestamp": time.time()
                            }
                        }
                        pub_socket.send_string("servers", zmq.SNDMORE)
                        pub_socket.send(msgpack.packb(response))
                        print(f"[T={clock}] 📤 Respondendo sincronização para {requester}")
            
            elif topic == "replication":
                # Replicação de dados
                key = data.get("key")
                value = data.get("value")
                if key and value:
                    db[key] = value
                    message_count += 1
                    print(f"[T={clock}] 📥 Replicação recebida: {key}")
                    
                    # Persistir
                    persist_data(key, value)
                    
                    # Sincronizar a cada 10 mensagens
                    if message_count % 10 == 0:
                        print(f"[T={clock}] 📊 10 mensagens processadas. Sincronizando relógio...")
                        threading.Thread(target=synchronize_clock, daemon=True).start()
        
        except Exception as e:
            print(f"[T={clock}] ❌ Erro no subscriber: {e}")
            time.sleep(1)

# Função de persistência
def persist_data(key, value):
    global clock
    log_dir = "/data"
    os.makedirs(log_dir, exist_ok=True)
    
    log_file = os.path.join(log_dir, f"{server_name}.log")
    
    try:
        with open(log_file, "a") as f:
            log_entry = f"[T={clock}] {time.time()} | {key} | {value}\n"
            f.write(log_entry)
    except Exception as e:
        print(f"[T={clock}] ❌ Erro ao persistir: {e}")

# Eleição de coordenador (Bully)
def elect_coordinator():
    global clock, coordinator_name
    
    clock += 1
    print(f"[T={clock}] 🗳️  Iniciando eleição de coordenador (Bully)...")
    
    # Obter lista de servidores
    servers_response = send_to_ref_server("list", {})
    
    if servers_response.get("status") == "OK":
        servers = servers_response.get("servers", [])
        
        if servers:
            # Ordenar por rank (maior rank = maior prioridade)
            servers_sorted = sorted(servers, key=lambda x: x.get("rank", 0), reverse=True)
            new_coordinator = servers_sorted[0].get("name")
            
            if new_coordinator != coordinator_name:
                coordinator_name = new_coordinator
                clock += 1
                
                # Anunciar novo coordenador
                if coordinator_name == server_name:
                    print(f"[T={clock}] 👑 EU SOU O COORDENADOR!")
                else:
                    print(f"[T={clock}] 👑 Coordenador eleito: {coordinator_name}")
                
                announcement = {
                    "coordinator": coordinator_name,
                    "clock": clock,
                    "timestamp": time.time()
                }
                pub_socket.send_string("servers", zmq.SNDMORE)
                pub_socket.send(msgpack.packb({"data": announcement}))

# Heartbeat
def heartbeat_loop():
    global clock
    
    while True:
        time.sleep(10)
        clock += 1
        
        response = send_to_ref_server("heartbeat", {"status": "alive"})
        
        if response.get("status") == "OK":
            print(f"[T={clock}] 💓 Heartbeat enviado")
        else:
            print(f"[T={clock}] ⚠️  Heartbeat falhou: {response.get('status')}")
        
        # Verificar se coordenador ainda está vivo
        servers_response = send_to_ref_server("list", {})
        if servers_response.get("status") == "OK":
            servers = servers_response.get("servers", [])
            server_names = [s.get("name") for s in servers]
            
            if coordinator_name and coordinator_name not in server_names:
                print(f"[T={clock}] ⚠️  Coordenador {coordinator_name} caiu! Iniciando nova eleição...")
                elect_coordinator()

# Inicialização
def main():
    global clock
    
    print(f"[T={clock}] 🚀 Servidor {server_name} iniciando...")
    
    # Registrar no servidor de referência
    clock += 1
    response = send_to_ref_server("register", {"name": server_name})
    
    if response.get("status") == "OK":
        rank = response.get("rank")
        print(f"[T={clock}] ✅ Registrado com rank {rank}")
    else:
        print(f"[T={clock}] ❌ Falha ao registrar: {response}")
    
    # Aguardar um pouco antes da eleição
    time.sleep(2)
    elect_coordinator()
    
    # Iniciar threads
    threading.Thread(target=subscriber_loop, daemon=True).start()
    threading.Thread(target=heartbeat_loop, daemon=True).start()
    
    print(f"[T={clock}] ✅ Servidor {server_name} rodando...")
    
    # Manter vivo
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print(f"\n[T={clock}] 🛑 Servidor {server_name} encerrando...")

if __name__ == "__main__":
    main()