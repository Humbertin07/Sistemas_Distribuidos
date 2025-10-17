import zmq
import time
import threading

USERNAME = "py_user_1"

def subscriber_thread(context, username):
    print(f"[SUB] Thread de inscrição iniciada para os tópicos 'general' e '{username}'...")
    sub_socket = context.socket(zmq.SUB)
    sub_socket.connect("tcp://message_server:5556")
    
    sub_socket.setsockopt_string(zmq.SUBSCRIBE, "general")
    sub_socket.setsockopt_string(zmq.SUBSCRIBE, username)

    while True:
        try:
            topic = sub_socket.recv_string()
            message = sub_socket.recv_string()
            print(f"\n{message}") 
            print("> ", end="", flush=True) 
        except zmq.ContextTerminated:
            break
        except Exception as e:
            print(f"[SUB] Erro: {e}")
            break

def main():
    print("Cliente principal iniciado...")
    context = zmq.Context()
    
    auth_socket = context.socket(zmq.REQ)
    auth_socket.connect("tcp://auth_server:5555")
    
    login_msg = f"login:{USERNAME}"
    print(f"Autenticando como '{USERNAME}'...")
    auth_socket.send_string(login_msg)
    response = auth_socket.recv_string()
    print(f"Resposta da autenticação: '{response}'")
    
    if not response.startswith("OK"):
        print("Falha na autenticação. Encerrando.")
        return

    sub_thread = threading.Thread(target=subscriber_thread, args=(context, USERNAME))
    sub_thread.daemon = True 
    sub_thread.start()

    pub_socket = context.socket(zmq.REQ)
    pub_socket.connect("tcp://message_server:5557")
    
    print("\nConectado! Instruções:")
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

            if message_text.startswith('/msg'):
                parts = message_text.split(' ', 2)
                if len(parts) < 3:
                    print("(Formato: /msg <usuario> <mensagem>)")
                    continue
                target_user = parts[1]
                message_content = f"{USERNAME} (privado): {parts[2]}"
                full_message = f"private:{target_user}:{message_content}"
            else:
                message_content = f"{USERNAME}: {message_text}"
                full_message = f"publish:general:{message_content}"
            
            pub_socket.send_string(full_message)
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