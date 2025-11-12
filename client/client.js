const zmq = require('zeromq');
const msgpack = require('msgpack5')();
const readline = require('readline');

class Client {
    constructor() {
        this.reqSocket = null;
        this.subSocket = null;
        this.logicalClock = 0;
        this.username = null;
        this.subscribedChannels = [];
        
        this.rl = readline.createInterface({
            input: process.stdin,
            output: process.stdout
        });
    }
    
    async init() {
        this.reqSocket = new zmq.Request();
        await this.reqSocket.connect('tcp://broker:5555');
        
        this.subSocket = new zmq.Subscriber();
        await this.subSocket.connect('tcp://proxy:5558');
        
        this.listenMessages();
    }
    
    incrementClock() {
        this.logicalClock++;
        return this.logicalClock;
    }
    
    updateClock(receivedClock) {
        this.logicalClock = Math.max(this.logicalClock, receivedClock || 0) + 1;
        return this.logicalClock;
    }
    
    async sendRequest(service, data) {
        this.incrementClock();
        const message = {
            service: service,
            data: {
                ...data,
                timestamp: new Date().toISOString(),
                clock: this.logicalClock
            }
        };
        
        await this.reqSocket.send(msgpack.encode(message));
        const [response] = await this.reqSocket.receive();
        const parsed = msgpack.decode(response);
        
        if (parsed.data && parsed.data.clock) {
            this.updateClock(parsed.data.clock);
        }
        
        return parsed;
    }
    
    async listenMessages() {
        for await (const [topic, msg] of this.subSocket) {
            try {
                const data = msgpack.decode(msg);
                const topicStr = topic.toString();
                
                // ✅ Só mostrar mensagens de tópicos que estou inscrito
                const isSubscribed = topicStr === this.username || 
                                   this.subscribedChannels.includes(topicStr);
                
                if (!isSubscribed) {
                    continue; // Ignorar mensagens de tópicos não inscritos
                }
                
                if (topicStr === this.username) {
                    console.log(`\n[PRIVADA] ${data.src}: ${data.message}`);
                    this.updateClock(data.clock);
                } else {
                    console.log(`\n[${topicStr}] ${data.user}: ${data.message}`);
                    this.updateClock(data.clock);
                }
                
                this.rl.prompt();
            } catch (e) {}
        }
    }
    
    async login(username) {
        const response = await this.sendRequest('login', { user: username });
        
        if (response.data.status === 'sucesso') {
            this.username = username;
            this.subSocket.subscribe(username);
            console.log('✅ Login realizado!');
            return true;
        } else {
            console.log('❌ Erro:', response.data.description);
            return false;
        }
    }
    
    async listUsers() {
        const response = await this.sendRequest('users', {});
        console.log('\n📋 Usuários:');
        response.data.users.forEach(user => console.log(`  - ${user}`));
    }
    
    async listChannels() {
        const response = await this.sendRequest('channels', {});
        console.log('\n📺 Canais:');
        response.data.channels.forEach(channel => {
            const sub = this.subscribedChannels.includes(channel) ? '✓' : ' ';
            console.log(`  [${sub}] ${channel}`);
        });
    }
    
    async createChannel(channelName) {
        const response = await this.sendRequest('channel', { channel: channelName });
        
        if (response.data.status === 'sucesso') {
            console.log('✅ Canal criado!');
        } else {
            console.log('❌ Erro:', response.data.description);
        }
    }
    
    async subscribeChannel(channelName) {
        const response = await this.sendRequest('channels', {});
        
        if (!response.data.channels.includes(channelName)) {
            console.log('❌ Canal não existe!');
            return;
        }
        
        if (this.subscribedChannels.includes(channelName)) {
            console.log('⚠️ Já inscrito!');
            return;
        }
        
        this.subSocket.subscribe(channelName);
        this.subscribedChannels.push(channelName);
        console.log(`✅ Inscrito em "${channelName}"`);
    }
    
    async sendPrivateMessage(dst, message) {
        const response = await this.sendRequest('message', {
            src: this.username,
            dst: dst,
            message: message
        });
        
        if (response.data.status === 'OK') {
            console.log('✅ Enviada!');
        } else {
            console.log('❌ Erro:', response.data.message);
        }
    }
    
    async publishToChannel(channel, message) {
        const response = await this.sendRequest('publish', {
            user: this.username,
            channel: channel,
            message: message
        });
        
        if (response.data.status === 'OK') {
            console.log('✅ Publicada!');
        } else {
            console.log('❌ Erro:', response.data.message);
        }
    }
    
    async start() {
        await this.init();
        
        console.log('╔════════════════════════════════════════╗');
        console.log('║  Sistema de Mensagens Instantâneas     ║');
        console.log('╚════════════════════════════════════════╝\n');
        
        this.rl.question('Digite seu nome: ', async (username) => {
            const success = await this.login(username);
            if (success) {
                this.showMenu();
            } else {
                process.exit(1);
            }
        });
    }
    
    showMenu() {
        console.log('\n╔════════════════════════════════════════╗');
        console.log('║              MENU                      ║');
        console.log('╠════════════════════════════════════════╣');
        console.log('║ 1. Listar usuários                     ║');
        console.log('║ 2. Listar canais                       ║');
        console.log('║ 3. Criar canal                         ║');
        console.log('║ 4. Inscrever-se em canal               ║');
        console.log('║ 5. Enviar mensagem privada             ║');
        console.log('║ 6. Publicar em canal                   ║');
        console.log('║ 7. Sair                                ║');
        console.log('╚════════════════════════════════════════╝\n');
        
        this.rl.question('Opção: ', async (option) => {
            console.log('');
            
            switch(option.trim()) {
                case '1':
                    await this.listUsers();
                    this.showMenu();
                    break;
                    
                case '2':
                    await this.listChannels();
                    this.showMenu();
                    break;
                    
                case '3':
                    this.rl.question('Nome do canal: ', async (name) => {
                        await this.createChannel(name);
                        this.showMenu();
                    });
                    break;
                    
                case '4':
                    this.rl.question('Canal: ', async (name) => {
                        await this.subscribeChannel(name);
                        this.showMenu();
                    });
                    break;
                    
                case '5':
                    this.rl.question('Destinatário: ', (dst) => {
                        this.rl.question('Mensagem: ', async (msg) => {
                            await this.sendPrivateMessage(dst, msg);
                            this.showMenu();
                        });
                    });
                    break;
                    
                case '6':
                    this.rl.question('Canal: ', (channel) => {
                        this.rl.question('Mensagem: ', async (msg) => {
                            await this.publishToChannel(channel, msg);
                            this.showMenu();
                        });
                    });
                    break;
                    
                case '7':
                    console.log('👋 Até logo!');
                    process.exit(0);
                    break;
                    
                default:
                    console.log('❌ Opção inválida!');
                    this.showMenu();
            }
        });
    }
}

const client = new Client();
client.start().catch(console.error);