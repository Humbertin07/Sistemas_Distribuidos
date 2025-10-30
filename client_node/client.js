import zmq from "zeromq";
import * as readline from "node:readline";
import { createInterface } from "node:readline/promises";
import { stdin as input, stdout as output } from "node:process";
import { encode, decode } from "@msgpack/msgpack";

const USERNAME = "node_user_1";
let clock = 0; 

console.log("Cliente Node.js (Relógios Lamport) iniciado...");

const authSocket = new zmq.Request();
const subSocket = new zmq.Subscriber();
const pubSocket = new zmq.Request();

const rl = createInterface({ input, output });

async function authenticate() {
  console.log(`Autenticando como '${USERNAME}'...`);
  await authSocket.connect("tcp://auth_server:5555");

  clock++;
  const authRequest = { 
    command: "login", 
    username: USERNAME,
    timestamp: clock 
  };
  const authRequestBytes = encode(authRequest);

  await authSocket.send(authRequestBytes);

  const [responseBytes] = await authSocket.receive();
  const responseData = decode(responseBytes);
  
  clock = Math.max(clock, responseData.timestamp) + 1;

  console.log(`[T=${clock}] Resposta da autenticação: '${responseData.status}: ${responseData.message}'`);
  return responseData.status === "OK";
}

async function runSubscriber() {
  console.log(`[SUB] Inscrevendo-se (Relógios) nos tópicos 'general' e '${USERNAME}'...`);
  subSocket.connect("tcp://message_server:5556");
  subSocket.subscribe("general");
  subSocket.subscribe(USERNAME);

  try {
    for await (const [topic, messageBytes] of subSocket) {
      const messageData = decode(messageBytes);
      
      clock = Math.max(clock, messageData.timestamp) + 1;

      readline.clearLine(process.stdout, 0);
      readline.cursorTo(process.stdout, 0);

      process.stdout.write(`(T=${clock}) ${messageData.message}\n`);
      rl.prompt(true);
    }
  } catch (err) {
    console.error("[SUB] Erro na thread de inscrição:", err);
  }
}

async function runMain() {
  if (!(await authenticate())) {
    console.log("Falha na autenticação. Encerrando.");
    process.exit(1);
  }

  await pubSocket.connect("tcp://message_server:5557");

  runSubscriber().catch(console.error);

  console.log("\nConectado! (Usando Relógios de Lamport)");
  console.log("Para canal público: <sua mensagem>");
  console.log("Para msg privada:  /msg <usuario_destino> <sua mensagem>");
  console.log("Para sair:         /sair");

  rl.prompt();

  rl.on("line", async (line) => {
    if (line.toLowerCase() === "/sair") {
      rl.close();
      return;
    }

    if (line) {
      let commandData = {};
      if (line.startsWith("/msg")) {
        const parts = line.split(" ");
        if (parts.length < 3) {
          console.log("(Formato: /msg <usuario> <mensagem>)");
          rl.prompt(true);
          return;
        }
        const targetUser = parts[1];
        const messageContent = parts.slice(2).join(" ");
        
        commandData = {
          command: "private",
          topic: targetUser,
          payload: `${USERNAME} (privado): ${messageContent}`
        };
      } else {
        commandData = {
          command: "publish",
          topic: "general",
          payload: `${USERNAME}: ${line}`
        };
      }
      
      clock++;
      commandData.timestamp = clock; 

      const commandBytes = encode(commandData);
      await pubSocket.send(commandBytes);

      const [response] = await pubSocket.receive(); 

      if (response.toString() !== "OK_ENVIADO") {
        console.log(`(Erro ao enviar: ${response.toString()})`);
      }
    }
    rl.prompt(true);
  });
  
  rl.on('close', () => {
    console.log("Fechando cliente.");
    authSocket.close();
    subSocket.close();
    pubSocket.close();
    process.exit(0);
  });
}

runMain().catch(console.error);