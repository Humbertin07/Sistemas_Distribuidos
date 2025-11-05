import zmq
import msgpack
import time

context = zmq.Context()

# Socket REP para receber requisições
rep_socket = context.socket(zmq.REP)
rep_socket.bind("tcp://*:5556")

# Socket PUB para publicar mensagens
pub_socket = context.socket(zmq.PUB)
pub_socket.connect("tcp://proxy:5557")

clock = 0
users = set()
channels = {"general"}  # Canal padrão

print("[Broker] 🚀 Iniciado na porta 5556")

def handle_request(request):
    global clock, users, channels
    
    clock = max(clock, request.get('clock', 0)) + 1
    
    command = request.get('command')
    response = {'clock': clock, 'timestamp': time.time()}
    
    try:
        if command == 'login':
            username = request.get('username')
            if username:
                users.add(username)
                response['status'] = 'OK'
                response['message'] = f'Bem-vindo, {username}!'
                print(f"[T={clock}] 👤 Login: {username}")
            else:
                response['status'] = 'ERROR'
                response['message'] = 'Username não fornecido'
        
        elif command == 'users':
            response['status'] = 'OK'
            response['users'] = list(users)
            print(f"[T={clock}] 👥 Listar usuários: {len(users)}")
        
        elif command == 'channel':
            channel = request.get('channel')
            if channel:
                channels.add(channel)
                response['status'] = 'OK'
                response['message'] = f'Canal {channel} criado'
                print(f"[T={clock}] 📢 Canal criado: {channel}")
            else:
                response['status'] = 'ERROR'
                response['message'] = 'Nome do canal não fornecido'
        
        elif command == 'channels':
            response['status'] = 'OK'
            response['channels'] = list(channels)
            print(f"[T={clock}] 📋 Listar canais: {len(channels)}")
        
        elif command == 'publish':
            topic = request.get('topic')
            user = request.get('user')
            payload = request.get('payload')
            
            if topic and user and payload:
                clock += 1
                message_data = {
                    'message': f'[{topic}] {user}: {payload}',
                    'timestamp': clock,
                    'user': user,
                    'topic': topic
                }
                
                # Publicar no proxy
                pub_socket.send_string(topic, zmq.SNDMORE)
                pub_socket.send(msgpack.packb(message_data))
                
                # Publicar para replicação
                replication_data = {
                    'key': f'{topic}_{clock}',
                    'value': message_data,
                    'clock': clock,
                    'timestamp': time.time()
                }
                pub_socket.send_string('replication', zmq.SNDMORE)
                pub_socket.send(msgpack.packb({'data': replication_data}))
                
                response['status'] = 'OK'
                response['message'] = 'Mensagem publicada'
                print(f"[T={clock}] 📤 Pub: {user} -> {topic}")
            else:
                response['status'] = 'ERROR'
                response['message'] = 'Dados incompletos para publicação'
        
        elif command == 'message':
            topic = request.get('topic')  # recipient
            user = request.get('user')
            payload = request.get('payload')
            
            if topic and user and payload:
                clock += 1
                message_data = {
                    'message': f'[{user} (privado)] {payload}',
                    'timestamp': clock,
                    'from': user,
                    'to': topic
                }
                
                # Publicar mensagem privada
                pub_socket.send_string(topic, zmq.SNDMORE)
                pub_socket.send(msgpack.packb(message_data))
                
                response['status'] = 'OK'
                response['message'] = 'Mensagem privada enviada'
                print(f"[T={clock}] 🔒 Msg privada: {user} -> {topic}")
            else:
                response['status'] = 'ERROR'
                response['message'] = 'Dados incompletos para mensagem privada'
        
        else:
            response['status'] = 'ERROR'
            response['message'] = f'Comando desconhecido: {command}'
            print(f"[T={clock}] ❌ Comando desconhecido: {command}")
    
    except Exception as e:
        response['status'] = 'ERROR'
        response['message'] = str(e)
        print(f"[T={clock}] ❌ Erro ao processar: {e}")
    
    return response

def main():
    print("[Broker] 👂 Aguardando requisições...")
    
    while True:
        try:
            # Receber requisição
            message = rep_socket.recv()
            request = msgpack.unpackb(message, raw=False)
            
            # Processar
            response = handle_request(request)
            
            # Enviar resposta
            rep_socket.send(msgpack.packb(response))
        
        except KeyboardInterrupt:
            print("\n[Broker] 🛑 Encerrando...")
            break
        except Exception as e:
            print(f"[Broker] ❌ Erro crítico: {e}")
            error_response = {
                'status': 'ERROR',
                'message': str(e),
                'clock': clock
            }
            try:
                rep_socket.send(msgpack.packb(error_response))
            except:
                pass

if __name__ == "__main__":
    main()