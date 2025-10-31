import zmq from "zeromq";
import * as readline from "node:readline";
import { createInterface } from "node:readline/promises";
import { stdin as input, stdout as output } from "node:process";
import { encode, decode } from "@msgpack/msgpack";

const USERNAME = "node_user_1";
let clock = 0; 

console.log("Cliente Node.js (Arquitetura Final) iniciado...");

const subSocket = new zmq.Subscriber();
const reqSocket = new zmq.Request();

const rl = createInterface({ input, output });

async function sendRequest(commandData) {
  clock++;
  commandData.timestamp = clock;
  
  await reqSocket.send(encode(commandData));
  const [responseBytes] = await reqSocket.receive();
  const responseData = decode(responseBytes);
  
  clock = Math.max(clock, responseData.clock) + 1;
  return responseData;
}

async function runSubscriber() {
  console.log(`[SUB] Conectando ao Proxy em tcp://proxy:5558`);
  subSocket.connect("tcp://proxy:5558");
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
  await reqSocket.connect("tcp://broker:5555");

  const authResponse = await sendRequest({ command: "login", username: USERNAME });
  console.log(`[T=${clock}] Resposta da autenticação: '${authResponse.status}'`);

  if (authResponse.status !== "OK") {
    console.log("Falha na autenticação. Encerrando.");
    process.exit(1);
  }

  runSubscriber().catch(console.error);

  console.log("\nConectado! Comandos:");
  console.log("/publicar <mensagem>");
  console.log("/msg <usuario> <mensagem>");
  console.log("/usuarios");
  console.log("/canais");
  console.log("/criar <canal>");
  console.log("/sair");

  rl.prompt();

  rl.on("line", async (line) => {
    if (line.toLowerCase() === "/sair") {
      rl.close();
      return;
    }
    
    let commandData = {};

    if (line.startsWith("/usuarios")) {
      const response = await sendRequest({ command: "users" });
      console.log(response.users);
    } else if (line.startsWith("/canais")) {
      const response = await sendRequest({ command: "channels" });
      console.log(response.channels);
    } else if (line.startsWith("/criar ")) {
      const channelName = line.split(" ", 2)[1];
      const response = await sendRequest({ command: "channel", "channel": channelName });
      console.log(response.status);
    } else if (line.startsWith("/msg ")) {
      const parts = line.split(" ");
      const targetUser = parts[1];
      const messageContent = parts.slice(2).join(" ");
      commandData = {
        command: "message",
        user: USERNAME,
        topic: targetUser,
        payload: `${USERNAME} (privado): ${messageContent}`
      };
      const response = await sendRequest(commandData);
      if (response.status !== "OK") console.log(`Erro: ${response.message}`);
    } else {
      commandData = {
        command: "publish",
        user: USERNAME,
        topic: "general",
        payload: `${USERNAME}: ${line}`
      };
      const response = await sendRequest(commandData);
      if (response.status !== "OK") console.log(`Erro: ${response.message}`);
    }
    
    rl.prompt(true);
  });
  
  rl.on('close', () => {
    console.log("Fechando cliente.");
    reqSocket.close();
    subSocket.close();
    process.exit(0);
  });
}

runMain().catch(console.error);