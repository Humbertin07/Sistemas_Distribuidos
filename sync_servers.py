#!/usr/bin/env python3
"""
Script para forçar sincronização completa entre os servidores.
Usa os dados do servidor com mais informações como fonte verdadeira.
"""

import json
import os
from collections import defaultdict

def load_json(path):
    """Carrega arquivo JSON"""
    try:
        with open(path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        return {} if 'channels.json' in path or 'users.json' in path else []

def save_json(path, data):
    """Salva arquivo JSON"""
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, 'w') as f:
        json.dump(data, f, indent=2)

def merge_data():
    """Mescla dados de todos os servidores"""
    
    servers = ['server1', 'server2', 'server3']
    base_path = '/workspaces/Sistemas_Distribuidos/data'
    
    # Coletar todos os dados
    all_users = {}
    all_channels = {}
    all_messages = []
    all_publications = []
    seen_msg_ids = set()
    seen_pub_ids = set()
    
    print("📊 Coletando dados de todos os servidores...")
    
    for server in servers:
        server_path = f"{base_path}/{server}"
        
        # Usuários
        users = load_json(f"{server_path}/users.json")
        for username, data in users.items():
            if username not in all_users:
                all_users[username] = data
        
        # Canais
        channels = load_json(f"{server_path}/channels.json")
        for channel_name, data in channels.items():
            if channel_name not in all_channels:
                all_channels[channel_name] = data
        
        # Mensagens
        messages = load_json(f"{server_path}/messages.json")
        for msg in messages:
            msg_id = msg.get('id')
            if msg_id and msg_id not in seen_msg_ids:
                all_messages.append(msg)
                seen_msg_ids.add(msg_id)
        
        # Publicações
        publications = load_json(f"{server_path}/publications.json")
        for pub in publications:
            pub_id = pub.get('id')
            if pub_id and pub_id not in seen_pub_ids:
                all_publications.append(pub)
                seen_pub_ids.add(pub_id)
    
    # Ordenar por timestamp/lamport_clock
    all_messages.sort(key=lambda x: (x.get('timestamp', ''), x.get('lamport_clock', 0)))
    all_publications.sort(key=lambda x: (x.get('timestamp', ''), x.get('lamport_clock', 0)))
    
    print(f"\n✅ Dados consolidados:")
    print(f"   - {len(all_users)} usuários únicos")
    print(f"   - {len(all_channels)} canais únicos")
    print(f"   - {len(all_messages)} mensagens únicas")
    print(f"   - {len(all_publications)} publicações únicas")
    
    # Salvar dados unificados em todos os servidores
    print("\n🔄 Sincronizando dados para todos os servidores...")
    
    for server in servers:
        server_path = f"{base_path}/{server}"
        
        save_json(f"{server_path}/users.json", all_users)
        save_json(f"{server_path}/channels.json", all_channels)
        save_json(f"{server_path}/messages.json", all_messages)
        save_json(f"{server_path}/publications.json", all_publications)
        
        print(f"   ✓ {server} atualizado")
    
    print("\n✨ Sincronização completa!")
    
    # Estatísticas por servidor (antes da sincronização)
    print("\n📈 Comparação de dados (antes da sincronização):")
    for server in servers:
        server_path = f"{base_path}/{server}"
        users = load_json(f"{server_path}/users.json")
        channels = load_json(f"{server_path}/channels.json")
        messages = load_json(f"{server_path}/messages.json")
        publications = load_json(f"{server_path}/publications.json")
        
        print(f"\n   {server}:")
        print(f"      - Usuários: {len(users)}")
        print(f"      - Canais: {len(channels)}")
        print(f"      - Mensagens: {len(messages)}")
        print(f"      - Publicações: {len(publications)}")

if __name__ == '__main__':
    merge_data()
