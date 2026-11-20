# Um Sistema de Monitoramento Orientado a Eventos – Parte 2

*10/11/2026*

## 1. Regras Gerais

1. Continuaremos com o mesmo projeto e mesmo grupo. A data de entrega é **07/12/2026**.
2. Os grupos farão uma apresentação do projeto para os colegas, durante a aula (similar ao primeiro trabalho).

## 2. Descrição

Na etapa 2 do projeto, assim como na etapa 1, vocês também devem implementar um sistema de monitoramento de tempo real, usando a plataforma **Apache Kafka** como infraestrutura de base. A etapa 2 será **100% continuidade** do trabalho anterior (etapa 1). Vocês também precisam estabelecer uma arquitetura que seja consistente com a arquitetura Kafka.

A principal diferença é que, na etapa 2, usaremos uma API, denominada **Kafka Streams**[^1], para implementar análises inteligentes de dados com base nas técnicas de **CEP**[^2], como discutimos em sala de aula.

## 3. Características

- O sistema deve herdar as características do domínio da etapa 1;
- Considerem uma arquitetura Web no desenvolvimento do projeto (back-end e front-end);
- Considerem um cliente (front-end) que permita visualização dos dados de forma amigável.

## 4. Funcionalidades

Funcionalidades adicionais para a etapa 2:

- Implementem o "evento composto" da etapa 1 usando Kafka Streams;
- Definam aplicações/topologias Kafka Streams que permitam a detecção de outras **3 situações de interesse** no sistema;
- Utilize a **álgebra de Allen** na detecção das suas situações (pelo menos uma);
- Usem operações **Stateless** e **Stateful** da DSL do Streams. Usem filtros, maps, joins, agregações, janelas temporais e/ou de eventos;
- Usem criatividade para incluir outras funcionalidades interessantes!

Divirtam-se!!! 🙂

---

[^1]: <https://kafka.apache.org/documentation/streams/>
[^2]: Complex Event Processing
