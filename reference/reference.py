import zmq
import msgpack
import time
from datetime import datetime, timedelta
import threading

class ReferenceServer:
    def __init__(self, port=5559):
        self.context = zmq.Context()
        self.socket = self.context.socket(zmq.REP)
        self.socket.bind(f"tcp://*:{port}")
        
        # Relógio lógico de Lamport
        self.lamport_clock = 0
        self.clock_lock = threading.Lock()
        
        self.servers = {}
        self.last_heartbeat = {}
        
        # Thread para limpar servidores inativos
        threading.Thread(target=self.cleanup_inactive_servers, daemon=True).start()
        
        print(f"[REFERENCE] Reference Server iniciado na porta {port}")
        print(f"[REFERENCE] Relógio lógico: {self.lamport_clock}")
    
    def increment_clock(self):
        """Incrementa o relógio lógico antes de enviar mensagens"""
        with self.clock_lock:
            self.lamport_clock += 1
            return self.lamport_clock
    
    def update_clock(self, received_clock):
        """Atualiza o relógio ao receber mensagens"""
        with self.clock_lock:
            self.lamport_clock = max(self.lamport_clock, received_clock) + 1
            return self.lamport_clock
    
    def cleanup_inactive_servers(self):
        """Remove servidores inativos (sem heartbeat há mais de 10 segundos)"""
        while True:
            time.sleep(5)
            current_time = time.time()
            
            inactive = []
            for server_id, last_time in self.last_heartbeat.items():
                if current_time - last_time > 10:
                    inactive.append(server_id)
            
            for server_id in inactive:
                if server_id in self.servers:
                    print(f"[REFERENCE] Removendo servidor inativo: {server_id}")
                    del self.servers[server_id]
                    del self.last_heartbeat[server_id]
    
    def handle_request(self, data):
        """Processa requisições e atualiza relógio lógico"""
        # Atualizar relógio com o recebido
        received_clock = data.get('lamport_clock', 0)
        current_clock = self.update_clock(received_clock)
        
        service = data.get('service')
        response = {}
        
        if service == 'register':
            response = self.handle_register(data)
        elif service == 'list_servers':
            response = self.handle_list_servers(data)
        elif service == 'heartbeat':
            response = self.handle_heartbeat(data)
        elif service == 'rank':
            response = self.handle_rank(data)
        else:
            response = {'status': 'error', 'message': 'Serviço desconhecido'}
        
        # Incrementar relógio antes de enviar resposta
        response['lamport_clock'] = self.increment_clock()
        
        return response
    
    def handle_register(self, data):
        """Registra um novo servidor"""
        server_id = data.get('server_id')
        address = data.get('address')
        port = data.get('port')
        
        self.servers[server_id] = {
            'server_id': server_id,
            'address': address,
            'port': port,
            'registered_at': datetime.now().isoformat(),
            'is_coordinator': False
        }
        
        self.last_heartbeat[server_id] = time.time()
        
        print(f"[REFERENCE] Servidor registrado: {server_id} ({address}:{port})")
        print(f"[REFERENCE] Total de servidores: {len(self.servers)}")
        
        return {
            'status': 'ok',
            'message': 'Servidor registrado com sucesso',
            'server_id': server_id,
            'total_servers': len(self.servers)
        }
    
    def handle_list_servers(self, data):
        """Lista todos os servidores registrados"""
        return {
            'status': 'ok',
            'servers': list(self.servers.values()),
            'total': len(self.servers)
        }
    
    def handle_heartbeat(self, data):
        """Atualiza o timestamp do último heartbeat"""
        server_id = data.get('server_id')
        is_coordinator = data.get('is_coordinator', False)
        
        if server_id in self.servers:
            self.last_heartbeat[server_id] = time.time()
            self.servers[server_id]['is_coordinator'] = is_coordinator
            
            # Resetar coordenador de outros se este é o novo
            if is_coordinator:
                for sid in self.servers:
                    if sid != server_id:
                        self.servers[sid]['is_coordinator'] = False
            
            return {
                'status': 'ok',
                'message': 'Heartbeat recebido',
                'servers': list(self.servers.values())
            }
        
        return {
            'status': 'error',
            'message': 'Servidor não registrado'
        }
    
    def handle_rank(self, data):
        """Retorna o ranking dos servidores por ID"""
        ranked = sorted(self.servers.values(), key=lambda x: x['server_id'], reverse=True)
        
        return {
            'status': 'ok',
            'servers': ranked,
            'total': len(ranked)
        }
    
    def run(self):
        """Loop principal do servidor"""
        print("[REFERENCE] Aguardando requisições...")
        
        while True:
            try:
                message = self.socket.recv()
                data = msgpack.unpackb(message)
                
                response = self.handle_request(data)
                
                packed_response = msgpack.packb(response)
                self.socket.send(packed_response)
                
            except Exception as e:
                print(f"[REFERENCE] Erro: {e}")
                error_response = msgpack.packb({
                    'status': 'error',
                    'message': str(e),
                    'lamport_clock': self.increment_clock()
                })
                self.socket.send(error_response)

if __name__ == '__main__':
    server = ReferenceServer()
    server.run()