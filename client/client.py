import zmq
import time

print("Cliente iniciado...")

context = zmq.Context()

print("Conectando ao servidor de autenticação...")
socket = context.socket(zmq.REQ) 
socket.connect("tcp://auth_server:5555")

for i in range(5):
    username = f"Humberto{i}"
    request_message = f"login:{username}"
    
    print(f"Enviando requisição: '{request_message}'")
    socket.send_string(request_message)

    response = socket.recv_string()
    print(f"Resposta recebida: '{response}'\n")
    
    time.sleep(2)

print("Cliente finalizou as requisições.")