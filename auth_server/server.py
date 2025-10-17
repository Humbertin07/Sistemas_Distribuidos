import zmq
import msgpack

print("Servidor de autenticação (MessagePack) iniciado...")

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5555")

while True:
    message_bytes = socket.recv()
    
    request_data = msgpack.unpackb(message_bytes, raw=False)
    
    print(f"Recebida requisição de autenticação: {request_data}")

    response_data = {}
    if request_data.get("command") == "login":
        username = request_data.get("username", "desconhecido")
        print(f"Usuário '{username}' autenticado com sucesso.")
        response_data = {
            "status": "OK",
            "message": f"Bem-vindo, {username}!"
        }
    else:
        print("Formato de requisição inválido.")
        response_data = {
            "status": "ERROR",
            "message": "Formato de login inválido."
        }
    
    response_bytes = msgpack.packb(response_data)
    socket.send(response_bytes)