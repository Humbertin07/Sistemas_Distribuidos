import zmq
import msgpack
import os
import json
from datetime import datetime

print("=== Servidor de Autenticação (MessagePack) ===")

DATA_DIR = "/data"
USERS_FILE = os.path.join(DATA_DIR, "users.json")
LOG_FILE = os.path.join(DATA_DIR, "auth.log")

# Garante que diretório existe
os.makedirs(DATA_DIR, exist_ok=True)

def load_users():
    """Carrega usuários do arquivo"""
    if os.path.exists(USERS_FILE):
        with open(USERS_FILE, 'r') as f:
            return json.load(f)
    return {}

def save_users(users):
    """Salva usuários no arquivo"""
    with open(USERS_FILE, 'w') as f:
        json.dump(users, f, indent=2)

def log_action(action, username, status):
    """Registra ações no log"""
    timestamp = datetime.utcnow().isoformat()
    log_entry = f"{timestamp} | {action} | {username} | {status}\n"
    with open(LOG_FILE, 'a') as f:
        f.write(log_entry)

# Carrega usuários existentes
users = load_users()
print(f"✓ {len(users)} usuários carregados")

context = zmq.Context()
socket = context.socket(zmq.REP)
socket.bind("tcp://*:5555")

print("✓ Servidor escutando na porta 5555")
print("🚀 Pronto para autenticação\n")

while True:
    try:
        message_bytes = socket.recv()
        request_data = msgpack.unpackb(message_bytes, raw=False)
        
        command = request_data.get("command")
        username = request_data.get("username", "unknown")
        
        print(f"📨 [{datetime.now().strftime('%H:%M:%S')}] {command}: {username}")
        
        response_data = {}
        
        if command == "login":
            # Registra/atualiza último acesso
            if username not in users:
                users[username] = {
                    "created_at": datetime.utcnow().isoformat(),
                    "login_count": 0
                }
            
            users[username]["last_login"] = datetime.utcnow().isoformat()
            users[username]["login_count"] = users[username].get("login_count", 0) + 1
            save_users(users)
            
            response_data = {
                "status": "OK",
                "message": f"Bem-vindo, {username}!",
                "login_count": users[username]["login_count"]
            }
            log_action("LOGIN", username, "SUCCESS")
            print(f"  ✓ Usuário autenticado (login #{users[username]['login_count']})")
            
        elif command == "register":
            password = request_data.get("password", "")
            
            if username in users:
                response_data = {
                    "status": "ERROR",
                    "message": "Usuário já existe"
                }
                log_action("REGISTER", username, "FAILED_EXISTS")
            else:
                users[username] = {
                    "created_at": datetime.utcnow().isoformat(),
                    "password_hash": password,  # Em produção, use hash real!
                    "login_count": 0
                }
                save_users(users)
                
                response_data = {
                    "status": "OK",
                    "message": f"Usuário {username} registrado com sucesso!"
                }
                log_action("REGISTER", username, "SUCCESS")
                print(f"  ✓ Novo usuário registrado")
        
        elif command == "list_users":
            user_list = list(users.keys())
            response_data = {
                "status": "OK",
                "users": user_list,
                "count": len(user_list)
            }
            print(f"  ✓ Lista de {len(user_list)} usuários enviada")
        
        else:
            response_data = {
                "status": "ERROR",
                "message": "Comando desconhecido"
            }
            log_action("UNKNOWN", username, f"COMMAND:{command}")
        
        response_bytes = msgpack.packb(response_data)
        socket.send(response_bytes)
        
    except KeyboardInterrupt:
        print("\n⏹ Servidor encerrado pelo usuário")
        break
    except Exception as e:
        print(f"❌ Erro: {e}")
        error_response = {
            "status": "ERROR",
            "message": f"Erro no servidor: {str(e)}"
        }
        socket.send(msgpack.packb(error_response))

socket.close()
context.term()