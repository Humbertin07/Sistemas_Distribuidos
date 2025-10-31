# Projeto: Sistema para troca de mensagem instantânea (BBS/IRC)

Este projeto é uma implementação de um sistema de mensagens instantâneas desenvolvido para a disciplina de Sistemas Distribuídos (CC7261).

## Arquitetura Final

O sistema segue a arquitetura de microsserviços definida nos enunciados do projeto, com 3 linguagens (Python, C# e Node.js). A arquitetura é separada em:

1.  **Middleware (Python):**
    * `ref_server`: Servidor de Referência para registro, listagem e heartbeat dos workers.
    * `broker`: Roteador ZMQ (Req-Rep) que faz o balanceamento de carga das requisições dos clientes para os workers.
    * `proxy`: Roteador ZMQ (Pub-Sub) que distribui as mensagens publicadas pelos workers para os clientes inscritos.
2.  **Workers (Python):**
    * `server`: O cérebro do sistema. Temos 3 réplicas deste worker. Ele implementa toda a lógica de negócios (login, chat, canais), persiste os dados e implementa os algoritmos de eleição e replicação.
3.  **Clientes (Python, Node.js, C#):**
    * `client_py`, `client_node`, `client_csharp`: Clientes interativos para os usuários.
    * `bot`: Cliente automático (2 réplicas) que gera mensagens em canais.

## Implementações de Conceitos

* **Padrões ZMQ:** `Request-Reply` e `Publish-Subscribe` são usados em toda a comunicação. O `broker` e `proxy` usam `ROUTER/DEALER` e `XSUB/XPUB`.
* **Serialização:** Toda a comunicação usa **MessagePack**.
* **Sincronização:**
    * **Relógios Lógicos (Lamport):** Todos os 4 componentes (`ref`, `server`, `client`, `bot`) implementam os relógios lógicos de Lamport (o `T=...`) para garantir a ordem dos eventos.
    * **Eleição (Bully):** O `server` (worker) implementa o algoritmo Bully. Ele periodicamente consulta o `ref_server`. Se o coordenador atual não estiver na lista ativa, o servidor de maior rank se auto-elege e publica no tópico `servers`.
* **Consistência e Replicação (Parte 5):**
    * **Método Escolhido:** Replicação "Publisher-Subscriber" com consistência eventual.
    * **Descrição:** O `broker` faz o balanceamento de carga (Round-Robin) entre os 3 servidores worker. Isso significa que cada worker recebe apenas uma parte das mensagens.
    * **Troca de Dados:** Para garantir que todos os servidores tenham todos os dados (requisito da `parte5.md`), cada servidor, ao processar uma mensagem (e salvá-la em seu log local), também a **publica** no tópico `replication`. Todos os outros servidores estão **inscritos** nesse tópico. Ao receberem um log de replicação, eles o salvam em seus próprios arquivos, garantindo que eventualmente todos os servidores tenham uma cópia de todos os logs.

---

### Como Rodar o Projeto Completo

1.  **Limpar o Ambiente (Se necessário):**
    ```bash
    docker compose down
    docker system prune -f
    ```

2.  **Construir e Subir TUDO (Middleware, Servidores e Bots):**
    Este comando irá construir as 3 réplicas do `server` e as 2 réplicas do `bot` e iniciá-las em background.
    ```bash
    docker compose up --build -d
    ```

3.  **Rodar os Clientes Interativos (Em terminais separados):**
    Você pode escolher quais clientes rodar.

    * **Para rodar o Cliente Python:**
        ```bash
        docker compose run --rm --build client_py
        ```
    * **Para rodar o Cliente Node.js:**
        ```bash
        docker compose run --rm --build client_node
        ```
    * **Para rodar o Cliente C#:**
        ```bash
        docker compose run --rm --build client_csharp
        ```

4.  **Para Parar Tudo:**
    ```bash
    docker compose down
    ```