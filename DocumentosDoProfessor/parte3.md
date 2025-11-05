# Troca de mensagens usando MessagePack

A terceira parte do projeto não envolve desenvolver uma parte nova do projeto, mas modificar o formato das mensagens que já são trocadas entre clientes e servidores.

Nas 2 primeiras partes do projeto trocamos as mensagens usando dicionários e para esta parte vamos fazer a troca do formato das mensagens para o MessagePack que é uma biblioteca de serialização em binário e pode ser usada para a troca de mensagens entre diferentes linguagens de programação. Com esta mudança, as mensagens podem se tornar menores sem a perda de informações ou de funcionalidades.

## Implementação no projeto

O [site do MessagePack](https://msgpack.org/) possui a documentação para as linguagens suportadas pela biblioteca. Procure a documentação das 3 linguagens que você está usando no seu projeto e faça a troca do padrão para troca de mensagens.

## Exemplo de uso relacionado a displina
O zerorpc é uma implementação de RPC (como o que usamos na aula de gRPC) usando ZeroMQ e MessagePack.
