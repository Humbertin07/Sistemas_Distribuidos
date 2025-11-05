# Projeto: Sistema para troca de mensagem instantânea

O Bulletin Board System (BBS) foi desenvolvido em no final da década de 1970 e permitia que usuários se conectassem aos servidores para terem acesso a notícias, participarem de discussões e trocarem mensagens com outros usuários. No final da década de 1980, o Internet Relay Chat (IRC) foi desenvolvido para tentar substituir uma parte do BBS, trazendo em parte as mesmas funcionalidades. Enquanto servidores de BBS existiram até a década de 1990, os de IRC ainda existem, mas perdendo usuários com o tempo.

Apesar destes serviços não serem tão usados como no passado, eles serviram como base de muitos serviços para troca de mensagens que são usados atualmente.

A proposta deste projeto é desenvolver uma versão simples de um destes sistemas usando o que estudamos na disciplina de sistemas distribuídos.

O projeto que será desenvolvido deverá permitir que usuários troquem mensagens privadas e postem em canais públicos, sendo que todas as interações dos usuários com o serviço deverão ser armazenados em disco, permitindo que o usuário recupere mensagens anteriores.

O desenvolvimento deste projeto terá algumas partes padronizadas (e.g., padrão para troca de mensagens, uso de containers para testes, biblioteca para troca de mensagem) e algumas partes que deverão ser escolhidas pelo desenvolvedor (e.g, linguagem utilizada, UI, formato de armazenamento dos dados).

## Partes padronizadas do Projeto

As partes padronizadas do projeto servem para garantir que todos conseguirão implementar o mesmo serviço e que seja possível fazer testes de interconexão entre os projetos (i.e., o cliente de um projeto pode se comunicar com o servidor de outro projeto). As padronizações são:
- uso do ZeroMQ para troca de mensagens;
- as mensagens deverão seguir o formato apresentado no enunciado da parte do projeto (que serão liberados durante as aulas);
- para facilitar os testes que faremos durante o desenvolvimento do projeto, deverá ser usado algum tipo de container (e.g., Docker ou Podman) para a execução de pelo menos uma instância de cada ator. Os nomes dos containers também serão padronizados e informados em cada parte do projeto.

## Partes em que a escolha é livre
- o ambiente de desenvolvimento e apresentação do projeto
- o formato de armazenamento dos dados (i.e., mensagens) trocadas entre os usuários e serviços
- a linguagem de programação usada é livre, **porém o projeto deve ser desenvolvido em pelo menos 3 linguagens, considerando que: C++ sem orientação a objetos será considerado como C e TypeScript será considerado como JavaScript por ser compilada para a outra linguagem**. Antes de iniciar o projeto, confirme se está tudo correto para não perder pontos na avaliação final.

## Formato da entrega do projeto
Para facilitar o desenvolvimento (inclusive no Github Codespace), o projeto será desenvolvido usando `git` e a entrega será apenas o link do repositório.

Como o projeto deste semestre será feito em partes durante as aulas, cada uma das partes do projeto deverá ser desenvolvida em uma branch diferente, com merge na `main` após o término da implementação de cada parte. Desta forma, espera-se que fique mais fácil de testar e verificar possíveis problemas durante o desenvolvimento do projeto.
