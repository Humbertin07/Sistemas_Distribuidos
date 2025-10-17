import zmq
import time
import threading
import msgpack

USERNAME = "py_user_1"

def subscriber_thread(context, username):
    print(f"[SUB] Thread de inscrição (MessagePack) iniciada para '{username}'...")
    sub_socket = context.socket(zmq.SUB)
    sub_socket.connect("tcp://message_server:5556")
    sub_socket.setsockopt_string(zmq.SUBSCRIBE, "general")
    sub_socket.setsockopt_string(zmq.SUBSCRIBE, username)

    while True:
        try:
            topic = sub_socket.recv_string()
            message_bytes = sub_socket.recv()
            message_data = msgpack.unpackb(message_bytes, raw=False)
            
            print(f"\n{message_data['message']}") 
            print("> ", end="", flush=True) 
        except zmq.ContextTerminated:
            break
        except Exception as e:
            print(f"[SUB] Erro: {e}")
            break

def main():
    print("Cliente principal (MessagePack) iniciado...")
    context = zmq.Context()
    
    auth_socket = context.socket(zmq.REQ)
    auth_socket.connect("tcp://auth_server:5555")
    
    auth_request = {"command": "login", "username": USERNAME}
    auth_request_bytes = msgpack.packb(auth_request)
    
    print(f"Autenticando como '{USERNAME}'...")
    auth_socket.send(auth_request_bytes)
    
    response_bytes = auth_socket.recv()
    response_data = msgpack.unpackb(response_bytes, raw=False)
    print(f"Resposta da autenticação: '{response_data['status']}: {response_data['message']}'")
    
    if response_data['status'] != "OK":
        print("Falha na autenticação. Encerrando.")
        return

    sub_thread = threading.Thread(target=subscriber_thread, args=(context, USERNAME))
    sub_thread.daemon = True 
    sub_thread.start()

    pub_socket = context.socket(zmq.REQ)
    pub_socket.connect("tcp://message_server:5557")
    
    print("\nConectado! (Usando MessagePack)")
    print("Para canal público: <sua mensagem>")
    print("Para msg privada:  /msg <usuario_destino> <sua mensagem>")
    print("Para sair:         /sair")

    try:
        while True:
            message_text = input("> ")
            
            if message_text.lower() == '/sair':
                break
                
            if not message_text:
                continue

            command_data = {}
            if message_text.startswith('/msg'):
                parts = message_text.split(' ', 2)
                if len(parts) < 3:
                    print("(Formato: /msg <usuario> <mensagem>)")
                    continue
                command_data = {
                    "command": "private",
                    "topic": parts[1],
                    "payload": f"{USERNAME} (privado): {parts[2]}"
                }
            else:
                command_data = {
                    "command": "publish",
                    "topic": "general",
                    "payload": f"{USERNAME}: {message_text}"
                }
            
            command_bytes = msgpack.packb(command_data)
            pub_socket.send(command_bytes)
            
            pub_response = pub_socket.recv_string() 
            
            if pub_response != "OK_ENVIADO":
                print(f"(Erro ao enviar: {pub_response})")

    except (KeyboardInterrupt, EOFError):
        print("\nSaindo...")
    finally:
        print("Fechando cliente.")
        context.term() 
        sub_thread.join() 

if __name__ == "__main__":
    main()