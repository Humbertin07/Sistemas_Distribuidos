import zmq
import time
import threading
import msgpack

USERNAME = "py_user_1"
clock = 0 

# --- Thread do Subscriber ---
def subscriber_thread(context, username):
    global clock
    print(f"[SUB] Conectando ao Proxy em tcp://proxy:5558")
    sub_socket = context.socket(zmq.SUB)
    sub_socket.connect("tcp://proxy:5558")
    sub_socket.setsockopt_string(zmq.SUBSCRIBE, "general")
    sub_socket.setsockopt_string(zmq.SUBSCRIBE, username)

    while True:
        try:
            topic = sub_socket.recv_string()
            message_bytes = sub_socket.recv()
            message_data = msgpack.unpackb(message_bytes, raw=False)
            
            received_clock = message_data.get('timestamp', 0)
            clock = max(clock, received_clock) + 1
            
            print(f"\r{' ' * 80}\r(T={clock}) {message_data['message']}") 
            print("> ", end="", flush=True) 
        except Exception as e:
            print(f"[SUB] Erro: {e}")
            break

# --- Função Principal ---
def main():
    global clock
    print("Cliente principal (Arquitetura Final) iniciado...")
    context = zmq.Context()
    
    # Conecta ao Broker
    req_socket = context.socket(zmq.REQ)
    req_socket.connect("tcp://broker:5555")

    def send_request(command_data):
        global clock
        clock += 1
        command_data["timestamp"] = clock
        
        req_socket.send(msgpack.packb(command_data))
        response_bytes = req_socket.recv()
        response_data = msgpack.unpackb(response_bytes, raw=False)
        
        received_clock = response_data.get('clock', 0)
        clock = max(clock, received_clock) + 1
        return response_data
    
    # --- Autenticação (Parte 1) ---
    auth_response = send_request({"command": "login", "username": USERNAME})
    print(f"[T={clock}] Resposta da autenticação: '{auth_response['status']}'")
    
    if auth_response['status'] != "OK":
        print("Falha na autenticação. Encerrando.")
        return

    # --- Inicia o Subscriber ---
    sub_thread = threading.Thread(target=subscriber_thread, args=(context, USERNAME))
    sub_thread.daemon = True 
    sub_thread.start()

    print("\nConectado! Comandos:")
    print("/publicar <mensagem>      (Envia no canal 'general')")
    print("/msg <usuario> <mensagem> (Envia mensagem privada)")
    print("/usuarios                 (Lista usuários)")
    print("/canais                   (Lista canais)")
    print("/criar <canal>            (Cria novo canal)")
    print("/sair")

    try:
        while True:
            message_text = input("> ")
            
            if message_text.lower() == '/sair':
                break
            if not message_text:
                continue

            command_data = {}
            
            if message_text.startswith('/usuarios'):
                response = send_request({"command": "users"})
                print(f"Usuários: {response.get('users')}")
                
            elif message_text.startswith('/canais'):
                response = send_request({"command": "channels"})
                print(f"Canais: {response.get('channels')}")
            
            elif message_text.startswith('/criar '):
                channel_name = message_text.split(' ', 1)[1]
                response = send_request({"command": "channel", "channel": channel_name})
                print(f"Criar canal: {response.get('status')}")

            elif message_text.startswith('/msg '):
                parts = message_text.split(' ', 2)
                command_data = {
                    "command": "message",
                    "user": USERNAME,
                    "topic": parts[1], # 'topic' é o usuário de destino
                    "payload": f"{USERNAME} (privado): {parts[2]}"
                }
                response = send_request(command_data)
                if response.get('status') != "OK": print(f"Erro: {response.get('message')}")

            else: # Default é publicar no 'general'
                command_data = {
                    "command": "publish",
                    "user": USERNAME,
                    "topic": "general",
                    "payload": f"{USERNAME}: {message_text}"
                }
                response = send_request(command_data)
                if response.get('status') != "OK": print(f"Erro: {response.get('message')}")

    except (KeyboardInterrupt, EOFError):
        print("\nSaindo...")
    finally:
        print("Fechando cliente.")
        context.term() 
        sub_thread.join() 

if __name__ == "__main__":
    main()