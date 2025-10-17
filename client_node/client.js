import zmq from "zeromq";
import * as readline from "node:readline/promises";
import { stdin as input, stdout as output } from "node:process";

const USERNAME = "node_user_1";

console.log("Cliente Node.js iniciado...");

const authSocket = new zmq.Request();
const subSocket = new zmq.Subscriber();
const pubSocket = new zmq.Request();

async function authenticate() {
  console.log(`Autenticando como '${USERNAME}'...`);
  await authSocket.connect("tcp://auth_server:5555");
  await authSocket.send(`login:${USERNAME}`);
  const [response] = await authSocket.receive();
  console.log(`Resposta da autenticação: '${response.toString()}'`);
  return response.toString().startsWith("OK");
}

async function runSubscriber() {
  console.log(`[SUB] Inscrevendo-se nos tópicos 'general' e '${USERNAME}'...`);
  subSocket.connect("tcp://message_server:5556");
  subSocket.subscribe("general");
  subSocket.subscribe(USERNAME);

  try {
    for await (const [topic, message] of subSocket) {
      process.stdout.write("\n"); 
      console.log(`${message.toString()}`);
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

  console.log("\nConectado! Instruções:");
  console.log("Para canal público: <sua mensagem>");
  console.log("Para msg privada:  /msg <usuario_destino> <sua mensagem>");
  console.log("Para sair:         /sair");

  const rl = readline.createInterface({ input, output });

  while (true) {
    const line = await rl.question("> ");

    if (line.toLowerCase() === "/sair") {
      break;
    }

    if (!line) continue;

    let fullMessage;
    if (line.startsWith("/msg")) {
      const parts = line.split(" ", 3);
      if (parts.length < 3) {
        console.log("(Formato: /msg <usuario> <mensagem>)");
        continue;
      }
      const targetUser = parts[1];
      const messageContent = `${USERNAME} (privado): ${parts[2]}`;
      fullMessage = `private:${targetUser}:${messageContent}`;
    } else {
      const messageContent = `${USERNAME}: ${line}`;
      fullMessage = `publish:general:${messageContent}`;
    }

    await pubSocket.send(fullMessage);
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