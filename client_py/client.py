import zmq
import msgpack
import sys
import time
import threading

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

    # ← CORREÇÃO: Adicionar timeouts
    req_socket.setsockopt(zmq.RCVTIMEO, 10000)  # 10 segundos
    req_socket.setsockopt(zmq.SNDTIMEO, 10000)  # 10 segundos

    def send_request(command_data):
        global clock
        clock += 1
        command_data["timestamp"] = clock
        
        try:
            req_socket.send(msgpack.packb(command_data))
            response_bytes = req_socket.recv()
            response_data = msgpack.unpackb(response_bytes, raw=False)
            
            received_clock = response_data.get('clock', 0)
            clock = max(clock, received_clock) + 1
            return response_data
        except zmq.Again:
            print(f"[T={clock}] ⏱️  Timeout: servidor não respondeu")
            return {"status": "TIMEOUT", "message": "Servidor não respondeu"}
        except zmq.ZMQError as e:
            print(f"[T={clock}] ❌ Erro ZMQ: {e}")
            return {"status": "ERROR", "message": str(e)}
        except Exception as e:
            print(f"[T={clock}] ❌ Erro: {e}")
            return {"status": "ERROR", "message": str(e)}

    def login(user):
        global USERNAME
        response = send_request({'command': 'login', 'username': user})
        if response.get('status') == 'OK':
            USERNAME = user
            print(f"✅ {response.get('message')}")
        else:
            print(f"❌ Erro no login: {response.get('message')}")
        return response

    def list_users():
        response = send_request({'command': 'users'})
        if response.get('status') == 'OK':
            users = response.get('users', [])
            print(f"👥 Usuários online ({len(users)}): {', '.join(users)}")
        else:
            print(f"❌ Erro: {response.get('message')}")
        return response

    def create_channel(channel_name):
        response = send_request({'command': 'channel', 'channel': channel_name})
        if response.get('status') == 'OK':
            print(f"✅ Canal '{channel_name}' criado")
        else:
            print(f"❌ Erro: {response.get('message')}")
        return response

    def list_channels():
        response = send_request({'command': 'channels'})
        if response.get('status') == 'OK':
            channels = response.get('channels', [])
            print(f"📋 Canais disponíveis ({len(channels)}): {', '.join(channels)}")
        else:
            print(f"❌ Erro: {response.get('message')}")
        return response

    def publish_message(topic, payload):
        if not USERNAME:
            print("❌ Erro: faça login primeiro! Use: login <username>")
            return
        
        response = send_request({
            'command': 'publish',
            'user': USERNAME,
            'topic': topic,
            'payload': payload
        })
        if response.get('status') == 'OK':
            print(f"✅ Mensagem publicada em '{topic}'")
        else:
            print(f"❌ Erro: {response.get('message')}")
        return response

    def send_private_message(recipient, payload):
        if not USERNAME:
            print("❌ Erro: faça login primeiro! Use: login <username>")
            return
        
        response = send_request({
            'command': 'message',
            'user': USERNAME,
            'topic': recipient,
            'payload': payload
        })
        if response.get('status') == 'OK':
            print(f"✅ Mensagem privada enviada para '{recipient}'")
        else:
            print(f"❌ Erro: {response.get('message')}")
        return response

    def subscribe(topic):
        print(f"\n📡 Para receber mensagens de '{topic}', abra outro terminal e execute:")
        print(f"   docker compose exec client python subscriber.py {topic}")
        print(f"\nOu se estiver fora do container:")
        print(f"   python subscriber.py {topic}\n")

    def print_help():
        print("\n" + "="*60)
        print("📋 COMANDOS DISPONÍVEIS")
        print("="*60)
        print("  login <username>           - Fazer login no sistema")
        print("  users                      - Listar usuários online")
        print("  channel <name>             - Criar um novo canal")
        print("  channels                   - Listar canais disponíveis")
        print("  publish <topic> <msg>      - Publicar mensagem em canal")
        print("  message <user> <msg>       - Enviar mensagem privada")
        print("  subscribe <topic>          - Ver como subscrever a um canal")
        print("  help                       - Mostrar esta ajuda")
        print("  exit                       - Sair do cliente")
        print("="*60 + "\n")

    print("\n" + "="*60)
    print("🚀 CLIENTE DE CHAT DISTRIBUÍDO")
    print("="*60)
    print("Digite 'help' para ver os comandos disponíveis\n")
    
    while True:
        try:
            prompt = f"[{USERNAME or 'guest'}]> "
            command = input(prompt).strip()
            
            if not command:
                continue
            
            parts = command.split(maxsplit=2)
            cmd = parts[0].lower()
            
            if cmd == "exit" or cmd == "quit":
                print("👋 Encerrando cliente...")
                break
            
            elif cmd == "help":
                print_help()
            
            elif cmd == "login":
                if len(parts) < 2:
                    print("❌ Uso: login <username>")
                else:
                    login(parts[1])
            
            elif cmd == "users":
                list_users()
            
            elif cmd == "channel":
                if len(parts) < 2:
                    print("❌ Uso: channel <name>")
                else:
                    create_channel(parts[1])
            
            elif cmd == "channels":
                list_channels()
            
            elif cmd == "publish":
                if len(parts) < 3:
                    print("❌ Uso: publish <topic> <mensagem>")
                else:
                    publish_message(parts[1], parts[2])
            
            elif cmd == "message":
                if len(parts) < 3:
                    print("❌ Uso: message <usuario> <mensagem>")
                else:
                    send_private_message(parts[1], parts[2])
            
            elif cmd == "subscribe":
                if len(parts) < 2:
                    print("❌ Uso: subscribe <topic>")
                else:
                    subscribe(parts[1])
            
            else:
                print(f"❌ Comando desconhecido: '{cmd}'")
                print("💡 Digite 'help' para ver os comandos disponíveis")
        
        except KeyboardInterrupt:
            print("\n👋 Encerrando cliente...")
            break
        except EOFError:
            print("\n👋 Encerrando cliente...")
            break
        except Exception as e:
            print(f"❌ Erro: {e}")
    
    req_socket.close()
    print("✅ Conexão encerrada")
    context.term() 
    sub_thread.join() 

if __name__ == "__main__":
    main()