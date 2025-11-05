# Consistência e replicação

Esta é a última parte do projeto e depende de todo o resto ter sido implementado (e testado) para funcionar. Nesta parte trataremos da réplica dos dados que são recebidos e armazenados pelos servidores.

## Problema
O broker como implementado para o projeto faz o balanceamento de carga entre os servidores usando o método de round-robin e com isso cada servidor tem uma parte das mensagens trocadas entre os clientes/bots.

Neste caso, se um servidor parar de funcionar temos a perda de uma parte do histórico de mensagens trocadas e se um cliente pedir o histórico de mesagens a um servidor, receberá apenas uma parte do histórico (o que está armazenado naquele servidor).

Portanto, é necessário fazer com que mais do que um servidor tenha a cópia dos dados. No caso deste projeto, todos os servidores devem possuir todos os dados.

## Implementação
O método de implementação, assim como o formato das mensagens, é livre. Porém é necessário documentar a escolha.

Adicione uma seção sobre esta parte no README.md do repositório do projeto, descreva o método escolhido dentre os que já existem e as modificações feitas neste método para funcionar no seu projeto. Documente também como é feita a troca de mensagens entre os servidores para garantir a troca dos dados.
