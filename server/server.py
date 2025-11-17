import zmq
import msgpack
import time
import json
import os
from datetime import datetime, timedelta
import uuid
import threading

class Server:
    def __init__(self, server_id, port=5555, reference_port=5559, replication_port=5560):
        print(f"[INIT] Iniciando servidor {server_id}...", flush=True)
        self.context = zmq.Context()
        self.server_id = server_id
        self.port = port
        self.reference_port = reference_port
        self.replication_port = replication_port
        print(f"[INIT] Contexto ZMQ criado", flush=True)
        
        # Relógio lógico de Lamport
        self.lamport_clock = 0
        self.clock_lock = threading.Lock()
        
        # Relógio físico para Berkeley
        self.physical_clock = time.time()
        self.clock_offset = 0.0
        
        # Controle de sincronização
        self.message_count = 0
        self.sync_interval = 10
        
        # Controle de eleição (Bully)
        self.is_coordinator = False
        self.coordinator_id = None
        self.election_in_progress = False
        self.election_lock = threading.Lock()
        
        # Dados
        self.users = {}
        self.channels = {}
        self.messages = []
        self.publications = []
        self.processed_ids = set()
        
        # Dados dos servidores
        self.servers = {}
        self.last_heartbeat = {}
        self.connected_servers = set()  # Track connected servers to avoid duplicates
        self.is_syncing = False  # Flag para evitar sincronização recursiva
        
        # Sockets
        self.socket = self.context.socket(zmq.REP)
        self.socket.connect("tcp://broker:5556")  # ✅ Conecta ao broker
        
        # Socket para Reference Server
        self.ref_socket = self.context.socket(zmq.REQ)
        self.ref_socket.connect(f"tcp://reference:5559")
        
        # Socket PUB para replicação entre servidores
        self.pub_socket = self.context.socket(zmq.PUB)
        self.pub_socket.bind(f"tcp://*:{self.replication_port}")
        
        # ✅ Socket PUB para publicar mensagens aos clientes via proxy
        self.proxy_pub_socket = self.context.socket(zmq.PUB)
        self.proxy_pub_socket.connect("tcp://proxy:5557")
        time.sleep(0.5)  # Aguardar conexão estabilizar
        
        # Socket SUB para receber replicações
        self.sub_socket = self.context.socket(zmq.SUB)
        self.sub_socket.setsockopt_string(zmq.SUBSCRIBE, "")
        
        # Socket REQ para comunicação entre servidores (eleição e sincronização)
        self.server_req_socket = self.context.socket(zmq.REQ)
        self.server_req_socket.setsockopt(zmq.RCVTIMEO, 1000)
        
        print(f"[INIT] Carregando dados...", flush=True)
        self.load_data()
        print(f"[INIT] Registrando com reference server...", flush=True)
        self.register_with_reference()
        
        # Iniciar threads
        print(f"[INIT] Iniciando threads...", flush=True)
        threading.Thread(target=self.send_heartbeat, daemon=True).start()
        threading.Thread(target=self.receive_replications, daemon=True).start()
        threading.Thread(target=self.monitor_coordinator, daemon=True).start()
        threading.Thread(target=self.periodic_sync, daemon=True).start()
        
        print(f"[SERVER-{self.server_id}] Servidor iniciado", flush=True)
        print(f"[SERVER-{self.server_id}] Conectado ao broker:5556", flush=True)
        print(f"[SERVER-{self.server_id}] Conectado ao proxy:5557")
    
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
    
    def get_physical_time(self):
        """Retorna o tempo físico ajustado"""
        return time.time() + self.clock_offset
    
    def check_sync_needed(self):
        """Verifica se precisa sincronizar após 10 mensagens"""
        self.message_count += 1
        if self.message_count >= self.sync_interval:
            self.message_count = 0
            if self.is_coordinator:
                threading.Thread(target=self.synchronize_clocks_berkeley, daemon=True).start()
            return True
        return False
    
    def synchronize_clocks_berkeley(self):
        """Algoritmo de Berkeley - coordenador sincroniza todos os servidores"""
        if not self.is_coordinator:
            return
        
        print(f"[SERVER-{self.server_id}] Iniciando sincronização de relógios (Berkeley)...")
        
        server_times = {}
        my_time = self.get_physical_time()
        server_times[self.server_id] = my_time
        
        active_servers = [s for s in self.servers.values() 
                         if s['server_id'] != self.server_id]
        
        for server in active_servers:
            try:
                sock = self.context.socket(zmq.REQ)
                sock.setsockopt(zmq.RCVTIMEO, 2000)
                # Use container name instead of stored address
                sock.connect(f"tcp://server{server['server_id']}:{server['port']}")
                
                clock = self.increment_clock()
                request = msgpack.packb({
                    'service': 'clock',
                    'lamport_clock': clock
                })
                
                t1 = time.time()
                sock.send(request)
                response = msgpack.unpackb(sock.recv())
                t2 = time.time()
                
                rtt = t2 - t1
                server_time = response['time'] + (rtt / 2)
                server_times[server['server_id']] = server_time
                
                sock.close()
                
            except Exception as e:
                print(f"[SERVER-{self.server_id}] Erro ao coletar tempo de {server['server_id']}: {e}")
        
        if len(server_times) > 0:
            avg_time = sum(server_times.values()) / len(server_times)
            
            adjustments = {}
            for sid, stime in server_times.items():
                adjustments[sid] = avg_time - stime
            
            print(f"[SERVER-{self.server_id}] Ajustes calculados: {adjustments}")
            
            self.clock_offset += adjustments[self.server_id]
            
            for server in active_servers:
                sid = server['server_id']
                if sid in adjustments:
                    try:
                        sock = self.context.socket(zmq.REQ)
                        sock.setsockopt(zmq.RCVTIMEO, 2000)
                        # Use container name instead of stored address
                        sock.connect(f"tcp://server{sid}:{server['port']}")
                        
                        clock = self.increment_clock()
                        request = msgpack.packb({
                            'service': 'adjust_clock',
                            'adjustment': adjustments[sid],
                            'lamport_clock': clock
                        })
                        
                        sock.send(request)
                        sock.recv()
                        sock.close()
                        
                    except Exception as e:
                        print(f"[SERVER-{self.server_id}] Erro ao enviar ajuste para {sid}: {e}")
            
            print(f"[SERVER-{self.server_id}] Sincronização concluída. Offset: {self.clock_offset:.6f}s")
    
    def start_election(self):
        """Inicia o algoritmo de eleição Bully"""
        with self.election_lock:
            if self.election_in_progress:
                return
            self.election_in_progress = True
        
        print(f"[SERVER-{self.server_id}] Iniciando eleição (Bully)...")
        
        higher_servers = [s for s in self.servers.values() 
                         if s['server_id'] > self.server_id]
        
        if not higher_servers:
            self.become_coordinator()
            return
        
        responses = []
        for server in higher_servers:
            try:
                sock = self.context.socket(zmq.REQ)
                sock.setsockopt(zmq.RCVTIMEO, 1500)
                # Use container name instead of stored address
                sock.connect(f"tcp://server{server['server_id']}:{server['port']}")
                
                clock = self.increment_clock()
                request = msgpack.packb({
                    'service': 'election',
                    'type': 'ELECTION',
                    'from': self.server_id,
                    'lamport_clock': clock
                })
                
                sock.send(request)
                response = msgpack.unpackb(sock.recv())
                responses.append(response)
                sock.close()
                
            except Exception as e:
                print(f"[SERVER-{self.server_id}] Servidor {server['server_id']} não respondeu")
        
        if responses:
            print(f"[SERVER-{self.server_id}] Servidores superiores responderam, aguardando novo coordenador...")
            time.sleep(3)
            
            if self.coordinator_id is None or self.coordinator_id <= self.server_id:
                self.start_election()
        else:
            self.become_coordinator()
    
    def become_coordinator(self):
        """Anuncia que este servidor é o novo coordenador"""
        print(f"[SERVER-{self.server_id}] Me tornei o COORDENADOR!")
        self.is_coordinator = True
        self.coordinator_id = self.server_id
        self.election_in_progress = False
        
        for server in self.servers.values():
            if server['server_id'] != self.server_id:
                try:
                    sock = self.context.socket(zmq.REQ)
                    sock.setsockopt(zmq.RCVTIMEO, 1000)
                    # Use container name instead of stored address
                    sock.connect(f"tcp://server{server['server_id']}:{server['port']}")
                    
                    clock = self.increment_clock()
                    request = msgpack.packb({
                        'service': 'election',
                        'type': 'COORDINATOR',
                        'coordinator_id': self.server_id,
                        'lamport_clock': clock
                    })
                    
                    sock.send(request)
                    sock.recv()
                    sock.close()
                    
                except Exception as e:
                    print(f"[SERVER-{self.server_id}] Erro ao anunciar coordenação para {server['server_id']}: {e}")
    
    def monitor_coordinator(self):
        """Monitora se o coordenador está ativo"""
        while True:
            time.sleep(5)
            
            if self.coordinator_id is None:
                self.start_election()
                continue
            
            if self.coordinator_id == self.server_id:
                continue
            
            coord = next((s for s in self.servers.values() 
                         if s['server_id'] == self.coordinator_id), None)
            
            if coord:
                last_seen = self.last_heartbeat.get(coord['server_id'], 0)
                if time.time() - last_seen > 10:
                    print(f"[SERVER-{self.server_id}] Coordenador {self.coordinator_id} inativo! Iniciando eleição...")
                    self.coordinator_id = None
                    self.start_election()
            else:
                print(f"[SERVER-{self.server_id}] Coordenador {self.coordinator_id} desconhecido! Iniciando eleição...")
                self.coordinator_id = None
                self.start_election()
    
    def handle_request(self, data):
        """Processa requisições e atualiza relógio lógico"""
        received_clock = data.get('lamport_clock', 0)
        if received_clock == 0:
            received_clock = data.get('data', {}).get('clock', 0)
        
        current_clock = self.update_clock(received_clock)
        
        service = data.get('service')
        
        # Serviços de sincronização e eleição
        if service == 'clock':
            return self.handle_clock_request(data)
        elif service == 'adjust_clock':
            return self.handle_adjust_clock(data)
        elif service == 'election':
            return self.handle_election_request(data)
        
        # ✅ Extrair dados da requisição
        service_data = data.get('data', {})
        
        # Serviços normais
        response = {}
        
        if service == 'login':
            response = self.handle_login(service_data)
        elif service == 'users':
            response = self.handle_list_users(service_data)
        elif service == 'channel':
            response = self.handle_create_channel(service_data)
        elif service == 'channels':
            response = self.handle_list_channels(service_data)
        elif service == 'message':
            response = self.handle_send_message(service_data)
        elif service == 'get_messages':
            response = self.handle_get_messages(service_data)
        elif service == 'publish':
            response = self.handle_publish(service_data)
        elif service == 'get_publications':
            response = self.handle_get_publications(service_data)
        elif service == 'sync_messages':
            response = self.handle_sync_messages(service_data)
        elif service == 'sync_publications':
            response = self.handle_sync_publications(service_data)
        else:
            response = {
                'service': service,
                'data': {
                    'status': 'error',
                    'message': 'Serviço desconhecido',
                    'clock': self.lamport_clock
                }
            }
        
        # Incrementar relógio antes de enviar resposta
        if 'data' in response and isinstance(response['data'], dict):
            response['data']['clock'] = self.increment_clock()
        else:
            response['lamport_clock'] = self.increment_clock()
        
        self.check_sync_needed()
        
        return response
    
    def handle_clock_request(self, data):
        """Responde com o tempo físico atual (Berkeley)"""
        current_clock = self.update_clock(data.get('lamport_clock', 0))
        response_clock = self.increment_clock()
        
        return {
            'status': 'ok',
            'time': self.get_physical_time(),
            'lamport_clock': response_clock
        }
    
    def handle_adjust_clock(self, data):
        """Aplica ajuste de relógio recebido do coordenador (Berkeley)"""
        current_clock = self.update_clock(data.get('lamport_clock', 0))
        adjustment = data.get('adjustment', 0.0)
        
        self.clock_offset += adjustment
        print(f"[SERVER-{self.server_id}] Relógio ajustado em {adjustment:.6f}s. Offset total: {self.clock_offset:.6f}s")
        
        response_clock = self.increment_clock()
        
        return {
            'status': 'ok',
            'lamport_clock': response_clock
        }
    
    def handle_election_request(self, data):
        """Processa mensagens de eleição (Bully)"""
        current_clock = self.update_clock(data.get('lamport_clock', 0))
        election_type = data.get('type')
        
        if election_type == 'ELECTION':
            from_server = data.get('from')
            print(f"[SERVER-{self.server_id}] Recebi pedido de eleição de {from_server}")
            
            threading.Thread(target=self.start_election, daemon=True).start()
            
            response_clock = self.increment_clock()
            return {
                'status': 'ok',
                'message': 'OK',
                'lamport_clock': response_clock
            }
        
        elif election_type == 'COORDINATOR':
            new_coordinator = data.get('coordinator_id')
            print(f"[SERVER-{self.server_id}] Novo coordenador anunciado: {new_coordinator}")
            
            self.coordinator_id = new_coordinator
            self.is_coordinator = (new_coordinator == self.server_id)
            self.election_in_progress = False
            
            response_clock = self.increment_clock()
            return {
                'status': 'ok',
                'lamport_clock': response_clock
            }
        
        response_clock = self.increment_clock()
        return {
            'status': 'error',
            'message': 'Tipo de eleição desconhecido',
            'lamport_clock': response_clock
        }
    
    def register_with_reference(self):
        """Registra este servidor no Reference Server"""
        try:
            clock = self.increment_clock()
            
            request = msgpack.packb({
                'service': 'register',
                'server_id': self.server_id,
                'address': 'server',
                'port': self.port,
                'lamport_clock': clock
            })
            
            self.ref_socket.send(request)
            response = msgpack.unpackb(self.ref_socket.recv())
            
            self.update_clock(response.get('lamport_clock', 0))
            
            if response.get('status') == 'ok':
                print(f"[SERVER-{self.server_id}] Registrado no Reference Server")
                self.update_server_list()
            
        except Exception as e:
            print(f"[SERVER-{self.server_id}] Erro ao registrar: {e}")
    
    def update_server_list(self):
        """Atualiza a lista de servidores do Reference Server"""
        try:
            clock = self.increment_clock()
            
            request = msgpack.packb({
                'service': 'list_servers',
                'lamport_clock': clock
            })
            
            self.ref_socket.send(request)
            response = msgpack.unpackb(self.ref_socket.recv())
            
            self.update_clock(response.get('lamport_clock', 0))
            
            if response.get('status') == 'ok':
                self.servers = {s['server_id']: s for s in response.get('servers', [])}
                
                # Connect to servers we haven't connected to yet
                for server in self.servers.values():
                    if server['server_id'] != self.server_id:
                        server_id = server['server_id']
                        if server_id not in self.connected_servers:
                            try:
                                address = f"tcp://server{server_id}:{self.replication_port}"
                                self.sub_socket.connect(address)
                                self.connected_servers.add(server_id)
                                print(f"[SERVER-{self.server_id}] Conectado ao servidor {server_id} para replicação em {address}")
                            except Exception as e:
                                print(f"[SERVER-{self.server_id}] Erro ao conectar ao servidor {server_id}: {e}")
                
                if self.coordinator_id is None and len(self.servers) > 0:
                    threading.Thread(target=self.start_election, daemon=True).start()
                    
        except Exception as e:
            print(f"[SERVER-{self.server_id}] Erro ao atualizar lista: {e}")
    
    def send_heartbeat(self):
        """Envia heartbeat periodicamente ao Reference Server"""
        while True:
            try:
                time.sleep(3)
                
                clock = self.increment_clock()
                
                request = msgpack.packb({
                    'service': 'heartbeat',
                    'server_id': self.server_id,
                    'is_coordinator': self.is_coordinator,
                    'lamport_clock': clock
                })
                
                self.ref_socket.send(request)
                response = msgpack.unpackb(self.ref_socket.recv())
                
                self.update_clock(response.get('lamport_clock', 0))
                
                if response.get('status') == 'ok':
                    servers_data = response.get('servers', [])
                    for server in servers_data:
                        sid = server['server_id']
                        self.last_heartbeat[sid] = time.time()
                        
                        # Conectar a novos servidores
                        if sid != self.server_id and sid not in self.connected_servers:
                            try:
                                address = f"tcp://server{sid}:{self.replication_port}"
                                self.sub_socket.connect(address)
                                self.connected_servers.add(sid)
                                print(f"[SERVER-{self.server_id}] Conectado ao servidor {sid} para replicação")
                            except Exception as e:
                                print(f"[SERVER-{self.server_id}] Erro ao conectar ao servidor {sid}: {e}")
                        
                        # Atualizar dict de servidores
                        if sid not in self.servers:
                            self.servers[sid] = server
                        
                        if server.get('is_coordinator'):
                            if self.coordinator_id != sid:
                                print(f"[SERVER-{self.server_id}] Coordenador atualizado: {sid}")
                                self.coordinator_id = sid
                                self.is_coordinator = (sid == self.server_id)
                
            except Exception as e:
                print(f"[SERVER-{self.server_id}] Erro no heartbeat: {e}")
    
    def receive_replications(self):
        """Recebe replicações de outros servidores - ✅ COMPLETO"""
        while True:
            try:
                message = self.sub_socket.recv()
                data = msgpack.unpackb(message)
                
                self.update_clock(data.get('lamport_clock', 0))
                
                msg_id = data.get('id')
                if msg_id in self.processed_ids:
                    continue
                
                self.processed_ids.add(msg_id)
                
                msg_type = data.get('type')
                
                if msg_type == 'message':
                    self.messages.append(data)
                    print(f"[SERVER-{self.server_id}] Mensagem replicada: {data.get('from')} -> {data.get('to')}")
                    
                elif msg_type == 'publication':
                    self.publications.append(data)
                    print(f"[SERVER-{self.server_id}] Publicação replicada em #{data.get('channel')}")
                
                # ✅ Replicação de login (SEMPRE atualiza para garantir sincronização)
                elif msg_type == 'login':
                    username = data.get('username')
                    self.users[username] = {
                        'username': username,
                        'logged_at': data.get('logged_at')
                    }
                    print(f"[SERVER-{self.server_id}] Login replicado: {username}")
                
                # ✅ Replicação de canal (SEMPRE atualiza para garantir sincronização)
                elif msg_type == 'channel':
                    channel_name = data.get('channel_name')
                    self.channels[channel_name] = {
                        'name': channel_name,
                        'created_at': data.get('created_at')
                    }
                    print(f"[SERVER-{self.server_id}] Canal replicado: {channel_name}")
                
                self.save_data()
                
            except Exception as e:
                print(f"[SERVER-{self.server_id}] Erro ao receber replicação: {e}")
    
    def replicate_data(self, data):
        """Replica dados para outros servidores"""
        try:
            data['lamport_clock'] = self.increment_clock()
            
            packed = msgpack.packb(data)
            self.pub_socket.send(packed)
            print(f"[SERVER-{self.server_id}] Dados replicados: {data.get('type')}")
        except Exception as e:
            print(f"[SERVER-{self.server_id}] Erro ao replicar: {e}")
    
    def sync_from_other_servers(self):
        """Sincroniza dados de todos os outros servidores ativos - ✅ COMPLETO"""
        if len(self.servers) == 0 or self.is_syncing:
            return  # Sem outros servidores ou já está sincronizando
        
        self.is_syncing = True  # Marcar que está sincronizando
        
        try:
            print(f"[SERVER-{self.server_id}] Sincronizando com {len(self.servers)-1} servidores...")
            
            users_synced = 0
            channels_synced = 0
            messages_synced = 0
            publications_synced = 0
            
            for server in self.servers.values():
                if server['server_id'] == self.server_id:
                    continue
                
                sock = None
                try:
                    # Criar socket temporário para pedir dados
                    sock = self.context.socket(zmq.REQ)
                    sock.setsockopt(zmq.RCVTIMEO, 2000)  # Timeout aumentado
                    sock.setsockopt(zmq.LINGER, 0)  # Não esperar ao fechar
                    sock.connect(f"tcp://server{server['server_id']}:{server['port']}")
                    
                    # ✅ Pedir usuários
                    request = msgpack.packb({
                        'service': 'users',
                        'data': {},
                        'lamport_clock': self.increment_clock()
                    })
                    sock.send(request)
                    response = msgpack.unpackb(sock.recv())
                    
                    if response.get('data', {}).get('users'):
                        for user in response['data']['users']:
                            if user not in self.users:
                                self.users[user] = {
                                    'username': user,
                                    'logged_at': datetime.now().isoformat()
                                }
                                users_synced += 1
                    
                    # ✅ Pedir canais
                    request = msgpack.packb({
                        'service': 'channels',
                        'data': {},
                        'lamport_clock': self.increment_clock()
                    })
                    sock.send(request)
                    response = msgpack.unpackb(sock.recv())
                    
                    if response.get('data', {}).get('channels'):
                        for channel in response['data']['channels']:
                            if channel not in self.channels:
                                self.channels[channel] = {
                                    'name': channel,
                                    'created_at': datetime.now().isoformat()
                                }
                                channels_synced += 1
                    
                    # ✅ Pedir mensagens (se aplicável - para servidores com poucos dados)
                    if len(self.messages) < 100:  # Só sincroniza se tiver menos de 100 mensagens
                        request = msgpack.packb({
                            'service': 'sync_messages',
                            'data': {},
                            'lamport_clock': self.increment_clock()
                        })
                        sock.send(request)
                        response = msgpack.unpackb(sock.recv())
                        
                        if response.get('data', {}).get('messages'):
                            for msg in response['data']['messages']:
                                msg_id = msg.get('id')
                                if msg_id and msg_id not in self.processed_ids:
                                    self.messages.append(msg)
                                    self.processed_ids.add(msg_id)
                                    messages_synced += 1
                    
                    # ✅ Pedir publicações (se aplicável - para servidores com poucos dados)
                    if len(self.publications) < 500:  # Só sincroniza se tiver menos de 500 publicações
                        request = msgpack.packb({
                            'service': 'sync_publications',
                            'data': {},
                            'lamport_clock': self.increment_clock()
                        })
                        sock.send(request)
                        response = msgpack.unpackb(sock.recv())
                        
                        if response.get('data', {}).get('publications'):
                            for pub in response['data']['publications']:
                                pub_id = pub.get('id')
                                if pub_id and pub_id not in self.processed_ids:
                                    self.publications.append(pub)
                                    self.processed_ids.add(pub_id)
                                    publications_synced += 1
                    
                except zmq.error.Again:
                    # Timeout - servidor pode estar ocupado
                    pass
                except Exception as e:
                    print(f"[SERVER-{self.server_id}] Erro ao sincronizar com server{server['server_id']}: {e}")
                finally:
                    if sock:
                        sock.close()
            
            if users_synced > 0 or channels_synced > 0 or messages_synced > 0 or publications_synced > 0:
                print(f"[SERVER-{self.server_id}] Sincronizados: {users_synced} usuários, {channels_synced} canais, {messages_synced} mensagens, {publications_synced} publicações")
                self.save_data()
        
        finally:
            self.is_syncing = False  # Liberar flag de sincronização
    
    def periodic_sync(self):
        """Sincronização periódica a cada 30 segundos"""
        print(f"[SERVER-{self.server_id}] Thread de sincronização periódica iniciada", flush=True)
        time.sleep(15)  # Espera inicial para os servidores se conectarem
        
        while True:
            try:
                if len(self.servers) > 0:  # Só sincroniza se conhecer outros servidores
                    self.sync_from_other_servers()
                time.sleep(30)  # Sincroniza a cada 30 segundos
            except Exception as e:
                print(f"[SERVER-{self.server_id}] Erro na sincronização periódica: {e}", flush=True)
    
    # ========== HANDLERS CORRIGIDOS ==========
    
    def handle_login(self, data):
        """Handler de login - ✅ CORRIGIDO COM REPLICAÇÃO"""
        username = data.get('user')  # Cliente envia 'user'
        
        if username in self.users:
            return {
                'service': 'login',
                'data': {
                    'status': 'erro',
                    'description': 'Usuário já existe',
                    'timestamp': datetime.now().isoformat(),
                    'clock': self.lamport_clock
                }
            }
        
        # Criar registro do usuário
        user_data = {
            'username': username,
            'logged_at': datetime.now().isoformat()
        }
        
        self.users[username] = user_data
        self.save_data()
        
        # ✅ Replicar login para outros servidores
        login_replica = {
            'id': str(uuid.uuid4()),
            'type': 'login',
            'username': username,
            'logged_at': user_data['logged_at'],
            'timestamp': datetime.now().isoformat(),
            'lamport_clock': self.lamport_clock
        }
        
        self.replicate_data(login_replica)
        print(f"[SERVER-{self.server_id}] Login de '{username}' replicado")
        
        return {
            'service': 'login',
            'data': {
                'status': 'sucesso',
                'timestamp': datetime.now().isoformat(),
                'clock': self.lamport_clock
            }
        }
    
    def handle_list_users(self, data):
        """Handler de listagem de usuários - ✅ COM SINCRONIZAÇÃO FORÇADA"""
        # Sincronizar com outros servidores ANTES de retornar (se não estiver já sincronizando)
        if not self.is_syncing:
            self.sync_from_other_servers()
        
        return {
            'service': 'users',
            'data': {
                'users': list(self.users.keys()),
                'timestamp': datetime.now().isoformat(),
                'clock': self.lamport_clock
            }
        }
    
    def handle_create_channel(self, data):
        """Handler de criação de canal - ✅ CORRIGIDO COM REPLICAÇÃO"""
        channel_name = data.get('channel')  # Cliente envia 'channel'
        
        if channel_name in self.channels:
            return {
                'service': 'channel',
                'data': {
                    'status': 'erro',
                    'description': 'Canal já existe',
                    'timestamp': datetime.now().isoformat(),
                    'clock': self.lamport_clock
                }
            }
        
        # Criar canal
        channel_data = {
            'name': channel_name,
            'created_at': datetime.now().isoformat()
        }
        
        self.channels[channel_name] = channel_data
        self.save_data()
        
        # ✅ Replicar criação de canal
        channel_replica = {
            'id': str(uuid.uuid4()),
            'type': 'channel',
            'channel_name': channel_name,
            'created_at': channel_data['created_at'],
            'timestamp': datetime.now().isoformat(),
            'lamport_clock': self.lamport_clock
        }
        
        self.replicate_data(channel_replica)
        print(f"[SERVER-{self.server_id}] Canal '{channel_name}' replicado")
        
        return {
            'service': 'channel',
            'data': {
                'status': 'sucesso',
                'timestamp': datetime.now().isoformat(),
                'clock': self.lamport_clock
            }
        }
    
    def handle_list_channels(self, data):
        """Handler de listagem de canais - ✅ COM SINCRONIZAÇÃO FORÇADA"""
        # Sincronizar com outros servidores ANTES de retornar (se não estiver já sincronizando)
        if not self.is_syncing:
            self.sync_from_other_servers()
        
        return {
            'service': 'channels',
            'data': {
                'channels': list(self.channels.keys()),
                'timestamp': datetime.now().isoformat(),
                'clock': self.lamport_clock
            }
        }
    
    def handle_send_message(self, data):
        """Handler de envio de mensagem - ✅ CORRIGIDO COM PROXY E VALIDAÇÃO"""
        dst_user = data.get('dst')
        src_user = data.get('src')
        
        # ✅ Validar se usuário destinatário existe - se não, tenta sincronizar primeiro
        if dst_user not in self.users:
            print(f"[SERVER-{self.server_id}] Usuário '{dst_user}' não encontrado, tentando sincronizar...")
            self.sync_from_other_servers()
            
            # Verifica novamente após sincronização
            if dst_user not in self.users:
                return {
                    'service': 'message',
                    'data': {
                        'status': 'erro',
                        'message': f'Usuário "{dst_user}" não existe',
                        'timestamp': datetime.now().isoformat(),
                        'clock': self.lamport_clock
                    }
                }
        
        message = {
            'id': str(uuid.uuid4()),
            'type': 'message',
            'from': src_user,
            'to': dst_user,
            'content': data.get('message'),
            'timestamp': datetime.now().isoformat(),
            'lamport_clock': self.lamport_clock
        }
        
        self.messages.append(message)
        self.processed_ids.add(message['id'])
        self.save_data()
        
        # Replicar para outros servidores
        self.replicate_data(message)
        
        # ✅ Publicar para o cliente destinatário via proxy
        msg_content = data.get('message')
        
        clock = self.increment_clock()
        pub_data = {
            'src': src_user,
            'message': msg_content,
            'timestamp': datetime.now().isoformat(),
            'clock': clock
        }
        
        try:
            self.proxy_pub_socket.send_string(dst_user, zmq.SNDMORE)
            self.proxy_pub_socket.send(msgpack.packb(pub_data))
            print(f"[SERVER-{self.server_id}] Mensagem publicada no proxy: {src_user} -> {dst_user}")
        except Exception as e:
            print(f"[SERVER-{self.server_id}] Erro ao publicar no proxy: {e}")
        
        return {
            'service': 'message',
            'data': {
                'status': 'OK',
                'timestamp': datetime.now().isoformat(),
                'clock': self.lamport_clock
            }
        }
    
    def handle_get_messages(self, data):
        """Handler de obtenção de mensagens"""
        username = data.get('username')
        
        user_messages = [
            msg for msg in self.messages
            if msg.get('to') == username or msg.get('from') == username
        ]
        
        return {
            'service': 'get_messages',
            'data': {
                'status': 'ok',
                'messages': user_messages,
                'timestamp': datetime.now().isoformat(),
                'clock': self.lamport_clock
            }
        }
    
    def handle_publish(self, data):
        """Handler de publicação - ✅ CORRIGIDO COM PROXY E VALIDAÇÃO"""
        channel = data.get('channel')
        user = data.get('user')
        
        # ✅ Validar se canal existe - se não, tenta sincronizar primeiro
        if channel not in self.channels:
            print(f"[SERVER-{self.server_id}] Canal '{channel}' não encontrado, tentando sincronizar...")
            self.sync_from_other_servers()
            
            # Verifica novamente após sincronização
            if channel not in self.channels:
                return {
                    'service': 'publish',
                    'data': {
                        'status': 'erro',
                        'message': f'Canal "{channel}" não existe',
                        'timestamp': datetime.now().isoformat(),
                        'clock': self.lamport_clock
                    }
                }
        
        publication = {
            'id': str(uuid.uuid4()),
            'type': 'publication',
            'channel': channel,
            'from': user,
            'content': data.get('message'),
            'timestamp': datetime.now().isoformat(),
            'lamport_clock': self.lamport_clock
        }
        
        self.publications.append(publication)
        self.processed_ids.add(publication['id'])
        self.save_data()
        
        # Replicar para outros servidores
        self.replicate_data(publication)
        
        # ✅ Publicar para clientes inscritos no canal via proxy
        msg_content = data.get('message')
        
        clock = self.increment_clock()
        pub_data = {
            'user': user,
            'message': msg_content,
            'timestamp': datetime.now().isoformat(),
            'clock': clock
        }
        
        try:
            self.proxy_pub_socket.send_string(channel, zmq.SNDMORE)
            self.proxy_pub_socket.send(msgpack.packb(pub_data))
            print(f"[SERVER-{self.server_id}] Publicação no canal '{channel}' enviada ao proxy")
        except Exception as e:
            print(f"[SERVER-{self.server_id}] Erro ao publicar no proxy: {e}")
        
        return {
            'service': 'publish',
            'data': {
                'status': 'OK',
                'timestamp': datetime.now().isoformat(),
                'clock': self.lamport_clock
            }
        }
    
    def handle_get_publications(self, data):
        """Handler de obtenção de publicações"""
        channel = data.get('channel')
        
        channel_pubs = [
            pub for pub in self.publications
            if pub.get('channel') == channel
        ]
        
        return {
            'service': 'get_publications',
            'data': {
                'status': 'ok',
                'publications': channel_pubs,
                'timestamp': datetime.now().isoformat(),
                'clock': self.lamport_clock
            }
        }
    
    def handle_sync_messages(self, data):
        """Handler para sincronização de mensagens entre servidores"""
        return {
            'service': 'sync_messages',
            'data': {
                'status': 'ok',
                'messages': self.messages,
                'timestamp': datetime.now().isoformat(),
                'clock': self.lamport_clock
            }
        }
    
    def handle_sync_publications(self, data):
        """Handler para sincronização de publicações entre servidores"""
        return {
            'service': 'sync_publications',
            'data': {
                'status': 'ok',
                'publications': self.publications,
                'timestamp': datetime.now().isoformat(),
                'clock': self.lamport_clock
            }
        }
    
    def load_data(self):
        """Carrega dados do disco"""
        try:
            if os.path.exists('/app/data/users.json'):
                with open('/app/data/users.json', 'r') as f:
                    self.users = json.load(f)
            
            if os.path.exists('/app/data/channels.json'):
                with open('/app/data/channels.json', 'r') as f:
                    self.channels = json.load(f)
            
            if os.path.exists('/app/data/messages.json'):
                with open('/app/data/messages.json', 'r') as f:
                    self.messages = json.load(f)
                    self.processed_ids = {msg.get('id') for msg in self.messages if 'id' in msg}
            
            if os.path.exists('/app/data/publications.json'):
                with open('/app/data/publications.json', 'r') as f:
                    self.publications = json.load(f)
                    self.processed_ids.update({pub.get('id') for pub in self.publications if 'id' in pub})
            
            print(f"[SERVER-{self.server_id}] Dados carregados do disco")
            
        except Exception as e:
            print(f"[SERVER-{self.server_id}] Erro ao carregar dados: {e}")
    
    def save_data(self):
        """Salva dados no disco"""
        try:
            os.makedirs('/app/data', exist_ok=True)
            
            with open('/app/data/users.json', 'w') as f:
                json.dump(self.users, f, indent=2)
            
            with open('/app/data/channels.json', 'w') as f:
                json.dump(self.channels, f, indent=2)
            
            with open('/app/data/messages.json', 'w') as f:
                json.dump(self.messages, f, indent=2)
            
            with open('/app/data/publications.json', 'w') as f:
                json.dump(self.publications, f, indent=2)
            
        except Exception as e:
            print(f"[SERVER-{self.server_id}] Erro ao salvar dados: {e}")
    
    def run(self):
        """Loop principal do servidor"""
        print(f"[SERVER-{self.server_id}] Aguardando requisições...")
        
        while True:
            try:
                message = self.socket.recv()
                data = msgpack.unpackb(message)
                
                response = self.handle_request(data)
                
                packed_response = msgpack.packb(response)
                self.socket.send(packed_response)
                
            except Exception as e:
                print(f"[SERVER-{self.server_id}] Erro: {e}")
                error_response = msgpack.packb({
                    'status': 'error',
                    'message': str(e),
                    'lamport_clock': self.increment_clock()
                })
                self.socket.send(error_response)

if __name__ == '__main__':
    import sys
    
    server_id = int(sys.argv[1]) if len(sys.argv) > 1 else 1
    port = int(sys.argv[2]) if len(sys.argv) > 2 else 5555
    
    server = Server(server_id=server_id, port=port)
    server.run()