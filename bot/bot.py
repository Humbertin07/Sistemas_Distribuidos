import zmq
import msgpack
import time
import random

context = zmq.Context()
clock = 0
bot_name = f"bot_{random.randint(1000, 9999)}"

req_socket = context.socket(zmq.REQ)
req_socket.connect("tcp://broker:5555")

sub_socket = context.socket(zmq.SUB)
sub_socket.connect("tcp://proxy:5558")
sub_socket.setsockopt_string(zmq.SUBSCRIBE, bot_name) # Ouve mensagens privadas
sub_socket.setsockopt_string(zmq.SUBSCRIBE, "general") 

print(f"Iniciando Bot: {bot_name}")

def send_request(service, data):
    global clock
    clock += 1
    req_data = {
        "command": service,
        "timestamp": clock,
        **data
    }
    req_socket.send(msgpack.packb(req_data))
    response_bytes = req_socket.recv()
    response_data = msgpack.unpackb(response_bytes, raw=False)
    
    clock = max(clock, response_data.get("clock", 0)) + 1
    return response_data

# 1. Login
response = send_request("login", {"username": bot_name})
print(f"[T={clock}] Bot {bot_name} logado: {response.get('status')}")
time.sleep(1)

# 2. Loop de envio de mensagens
messages = [
    "Olá, mundo!", "Testando...", "Alguém aí?", "Este é um bot.",
    "Que dia é hoje?", "Amo Sistemas Distribuídos!", "ZMQ é legal.",
    "Python é top.", "Enviando mensagem...", "Fim do teste."
]

while True:
    try:
        # Pega a lista de canais
        channels_response = send_request("channels", {})
        channels = channels_response.get("channels", ["general"])
        
        # Escolhe um canal aleatório
        channel = random.choice(channels)
        
        print(f"[T={clock}] Bot {bot_name} vai enviar 10 mensagens para o canal '{channel}'")
        
        for i in range(10):
            message = random.choice(messages)
            payload = f"{bot_name} (bot): {message}"
            
            send_request("publish", {
                "user": bot_name,
                "topic": channel,
                "payload": payload
            })
            time.sleep(random.randint(1, 5)) # Espera entre 1-5s
            
    except Exception as e:
        print(f"Erro no bot: {e}")
        time.sleep(10) # Espera 10s se der erro