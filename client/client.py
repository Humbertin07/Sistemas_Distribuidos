import zmq
import time
import threading

def subscriber_thread(context):
    """Esta função roda em uma thread separada para escutar o canal."""
    print("[SUB] Thread de inscrição iniciada...")
    sub_socket = context.socket(zmq.SUB)
    sub_socket.connect("tcp://message_server:5556")
    
    sub_socket.setsockopt_string(zmq.SUBSCRIBE, "general")

    while True:
        try:
            topic = sub_socket.recv_string()
            message = sub_socket.recv_string()
            print(f"\n[CANAL RECEBIDO] {message}\n> ", end="")
        except zmq.ContextTerminated:
            print("[SUB] Contexto encerrado, fechando thread de inscrição.")
            break

def main():
    print("Cliente principal iniciado...")
    context = zmq.Context()
    
    auth_socket = context.socket(zmq.REQ)
    auth_socket.connect("tcp://auth_server:5555")
    
    username = "Humberto"
    login_msg = f"login:{username}"
    print(f"Autenticando como '{username}'...")
    auth_socket.send_string(login_msg)
    response = auth_socket.recv_string()
    print(f"Resposta da autenticação: '{response}'")
    
    if not response.startswith("OK"):
        print("Falha na autenticação. Encerrando.")
        return

    sub_thread = threading.Thread(target=subscriber_thread, args=(context,))
    sub_thread.daemon = True 
    sub_thread.start()

    pub_socket = context.socket(zmq.REQ)
    pub_socket.connect("tcp://message_server:5557")
    
    print("\nConectado! Você está no canal 'general'.")
    print("Digite suas mensagens e pressione Enter para enviar.")
    print("Digite 'sair' para fechar o cliente.")

    try:
        while True:
            message_text = input("> ")
            
            if message_text.lower() == 'sair':
                break
                
            if not message_text:
                continue

            full_message = f"{username}: {message_text}"
            
            pub_socket.send_string(full_message)
            
            pub_response = pub_socket.recv_string()
            if pub_response == "OK_PUBLICADO":
                print(f"(Mensagem enviada!)\n> ", end="")
            else:
                print(f"(Erro ao enviar: {pub_response})\n> ", end="")

    except KeyboardInterrupt:
        print("\nSaindo...")
    finally:
        print("Fechando cliente.")
        context.term() 
        sub_thread.join() 

if __name__ == "__main__":
    main()