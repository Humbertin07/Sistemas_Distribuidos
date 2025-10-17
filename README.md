# Projeto: Sistema para troca de mensagem instantânea (BBS/IRC)

Este projeto é uma implementação de um sistema de mensagens instantâneas desenvolvido para a disciplina de Sistemas Distribuídos (CC7261).

## Descrição

O sistema permite que usuários troquem mensagens privadas e participem de canais públicos, utilizando conceitos de sistemas distribuídos como ZeroMQ para comunicação e contêineres para implantação.

### Parte 1: Autenticação com Request-Reply
- **Status:** Concluído
- **Componentes:** `auth_server` e `client` em Python.
- **Padrão de Comunicação:** Request-Reply com ZeroMQ.