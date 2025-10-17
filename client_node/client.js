import zmq from "zeromq";
import * as readline from "node:readline";
import { createInterface } from "node:readline/promises";
import { stdin as input, stdout as output } from "node:process";
import { encode, decode } from "@msgpack/msgpack";

const USERNAME = "node_user_1";

console.log("Cliente Node.js (MessagePack) iniciado...");

const authSocket = new zmq.Request();
const subSocket = new zmq.Subscriber();
const pubSocket = new zmq.Request();

async function authenticate() {
  console.log(`Autenticando como '${USERNAME}'...`);
  await authSocket.connect("tcp://auth_server:5555");

  const authRequest = { command: "login", username: USERNAME };
  const authRequestBytes = encode(authRequest);

  await authSocket.send(authRequestBytes);

  const [responseBytes] = await authSocket.receive();
  const responseData = decode(responseBytes);

  console.log(`Resposta da autenticação: '${responseData.status}: ${responseData.message}'`);
  return responseData.status === "OK";
}

async function runSubscriber() {
  console.log(`[SUB] Inscrevendo-se (MessagePack) nos tópicos 'general' e '${USERNAME}'...`);
  subSocket.connect("tcp://message_server:5556");
  subSocket.subscribe("general");
  subSocket.subscribe(USERNAME);

  try {
    for await (const [topic, messageBytes] of subSocket) {
      const messageData = decode(messageBytes);

      readline.clearLine(process.stdout, 0);
      readline.cursorTo(process.stdout, 0);

      console.log(`${messageData.message}`);

      process.stdout.write("> ");
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

  console.log("\nConectado! (Usando MessagePack)");
  console.log("Para canal público: <sua mensagem>");
  console.log("Para msg privada:  /msg <usuario_destino> <sua mensagem>");
  console.log("Para sair:         /sair");

  const rl = createInterface({ input, output });

  while (true) {
    const line = await rl.question("> ");

    if (line.toLowerCase() === "/sair") {
      break;
    }

    if (!line) continue;

    let commandData = {};
    if (line.startsWith("/msg")) {
      const parts = line.split(" ", 3);
      if (parts.length < 3) {
        console.log("(Formato: /msg <usuario> <mensagem>)");
        continue;
      }
      commandData = {
        command: "private",
        topic: parts[1],
        payload: `${USERNAME} (privado): ${parts[2]}`
      };
    } else {
      commandData = {
        command: "publish",
        topic: "general",
        payload: `${USERNAME}: ${line}`
      };
    }

    const commandBytes = encode(commandData);
    await pubSocket.send(commandBytes);

    const [response] = await pubSocket.receive();

    if (response.toString() !== "OK_ENVIADO") {
      console.log(`(Erro ao enviar: ${response.toString()})`);
    }
  }

  console.log("Fechando cliente.");
  rl.close();
  authSocket.close();
  subSocket.close();
  pubSocket.close();
  process.exit(0);
}

runMain().catch(console.error);