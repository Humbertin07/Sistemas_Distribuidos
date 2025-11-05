# Sistema de Mensagens Instantâneas Distribuído

Sistema completo de troca de mensagens usando ZeroMQ, implementado em **3 linguagens**: Python, JavaScript/Node.js e C++.

## 🎯 Linguagens Utilizadas

1. **Python** - Server, Broker, Proxy, Reference Server, Bot
2. **JavaScript/Node.js** - Cliente interativo
3. **C++** - Bot automático

## ✨ Funcionalidades

### Parte 1: Request-Reply
- ✅ Login de usuários
- ✅ Listagem de usuários cadastrados
- ✅ Criação e listagem de canais
- ✅ Persistência em disco

### Parte 2: Publisher-Subscriber
- ✅ Publicação em canais públicos
- ✅ Mensagens privadas entre usuários
- ✅ Bots automáticos

### Parte 3: MessagePack
- ✅ Serialização binária de todas as mensagens

### Parte 4: Relógios
- ✅ Relógio lógico de Lamport
- ✅ Sincronização com Algoritmo de Berkeley
- ✅ Eleição de coordenador (Bully)
- ✅ Reference Server

### Parte 5: Replicação
- ✅ Replicação entre servidores
- ✅ Consistência eventual
- ✅ Tolerância a falhas

## 🚀 Como Executar

### Pré-requisitos
- Docker
- Docker Compose

### Comandos

```bash
# Construir e iniciar todos os containers
docker-compose up --build

# Executar em background
docker-compose up -d

# Ver logs
docker-compose logs -f

# Cliente interativo
docker-compose run --rm client

# Parar tudo
docker-compose down

# Limpar volumes
docker-compose down -v
```

## 📊 Arquitetura

```
Cliente/Bot → Broker (REQ-REP) → Servidor
    ↓                               ↓
Proxy (PUB-SUB) ←──────────────────┘
    ↓
Cliente/Bot (recebe mensagens)

Servidores ↔ Reference Server
Servidores ↔ Servidores (replicação)
```

## 🔌 Portas

- **5555**: Broker frontend (clientes)
- **5556**: Broker backend (servidores)
- **5557**: Proxy XSUB
- **5558**: Proxy XPUB
- **5559**: Reference Server
- **5560**: Replicação entre servidores

## 📖 Uso do Cliente

Ao executar o cliente:

```
1. Listar usuários
2. Listar canais
3. Criar canal
4. Inscrever-se em canal
5. Enviar mensagem privada
6. Publicar em canal
7. Sair
```

## 🔄 Método de Replicação

**Replicação Passiva com Consistência Eventual**

1. Servidor recebe mensagem do cliente
2. Armazena localmente
3. Publica para outros servidores
4. Outros servidores recebem e armazenam
5. IDs únicos evitam duplicatas

## 👨‍💻 Autor

**Humberto Pellegrini**
- GitHub: [@Humbertin07](https://github.com/Humbertin07)
- Faculdade: FEI
- Disciplina: Sistemas Distribuídos

## 📄 Licença

Projeto acadêmico - 2025