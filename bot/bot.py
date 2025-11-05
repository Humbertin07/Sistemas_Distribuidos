import zmq
import msgpack
import time
import random
import uuid
from datetime import datetime

class Bot:
    def __init__(self):
        self.context = zmq.Context()
        
        self.req_socket = self.context.socket(zmq.REQ)
        self.req_socket.connect("tcp://broker:5555")
        
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.connect("tcp://proxy:5558")
        
        self.username = f"bot_py_{uuid.uuid4().hex[:6]}"
        self.logical_clock = 0
        
        self.messages = [
            "Olá! 👋",
            "Como estão?",
            "Bom dia! ☀️",
            "Bot Python funcionando! 🐍",
            "Teste automático",
            "Mensagem #%d",
            "Tudo ok!",
            "Sistema operacional! ⚙️"
        ]
    
    def increment_clock(self):
        self.logical_clock += 1
        return self.logical_clock
    
    def update_clock(self, received_clock):
        self.logical_clock = max(self.logical_clock, received_clock or 0) + 1
        return self.logical_clock
    
    def send_request(self, service, data):
        self.increment_clock()
        message = {
            "service": service,
            "data": {
                **data,
                "timestamp": datetime.now().isoformat(),
                "clock": self.logical_clock
            }
        }
        
        self.req_socket.send(msgpack.packb(message))
        response = msgpack.unpackb(self.req_socket.recv())
        
        if response.get("data", {}).get("clock"):
            self.update_clock(response["data"]["clock"])
        
        return response
    
    def login(self):
        print(f"[{self.username}] Login...")
        response = self.send_request("login", {"user": self.username})
        
        if response["data"]["status"] == "sucesso":
            print(f"[{self.username}] Conectado!")
            self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, self.username)
            return True
        else:
            self.username = f"bot_py_{uuid.uuid4().hex[:6]}"
            return self.login()
    
    def get_channels(self):
        response = self.send_request("channels", {})
        return response["data"]["channels"]
    
    def create_channels(self):
        for channel in ["geral", "testes", "python"]:
            try:
                self.send_request("channel", {"channel": channel})
            except:
                pass
    
    def publish(self, channel, message):
        response = self.send_request("publish", {
            "user": self.username,
            "channel": channel,
            "message": message
        })
        return response["data"]["status"] == "OK"
    
    def run(self):
        if not self.login():
            return
        
        time.sleep(1)
        self.create_channels()
        
        time.sleep(1)
        channels = self.get_channels()
        
        if not channels:
            return
        
        print(f"[{self.username}] Iniciado! Canais: {channels}")
        
        count = 0
        
        while True:
            try:
                channel = random.choice(channels)
                
                for i in range(10):
                    msg = random.choice(self.messages)
                    if "%d" in msg:
                        msg = msg % (count + 1)
                    
                    if self.publish(channel, msg):
                        count += 1
                        print(f"[{self.username}] #{count} → {channel}: {msg}")
                    
                    time.sleep(random.uniform(0.5, 2.0))
                
                time.sleep(random.uniform(2.0, 5.0))
                
            except KeyboardInterrupt:
                break
            except Exception as e:
                print(f"[{self.username}] Erro: {e}")
                time.sleep(2)

if __name__ == "__main__":
    bot = Bot()
    time.sleep(5)
    bot.run()