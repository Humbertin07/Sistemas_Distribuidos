import zmq
import time

print("Servidor de autenticação iniciado...")

context = zmq.Context()
socket = context.socket(zmq.REP) 
socket.bind("tcp://*:5555")

while True:
    message = socket.recv_string()
    print(f"Recebida requisição de autenticação: {message}")

    user_data = message.split(':')
    if len(user_data) == 2 and user_data[0] == "login":
        username = user_data[1]
        print(f"Usuário '{username}' autenticado com sucesso.")
        socket.send_string(f"OK: Bem-vindo, {username}!")
    else:
        print("Formato de requisição inválido.")
        socket.send_string("ERROR: Formato de login inválido. Use 'login:seu_nome'.")

    time.sleep(1)