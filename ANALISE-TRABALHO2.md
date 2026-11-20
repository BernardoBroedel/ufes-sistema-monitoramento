# Análise do Trabalho 2: Kafka Streams + CEP

Análise do enunciado (`TRABALHO2.md`) comparado ao estado atual do projeto da etapa 1.

## 1. O que o enunciado pede

| # | Requisito | Situação atual |
|---|---|---|
| R1 | Reimplementar o **evento composto da etapa 1** (F4, estagnação) com **Kafka Streams** | F4 hoje é um `KafkaConsumer` comum, com duas threads e estado em memória (`ConcurrentSkipListMap` + `HistoricoEstacoes`) |
| R2 | **3 novas situações de interesse** feitas com topologias Streams | Não existem |
| R3 | **Álgebra de Allen** em pelo menos uma situação | Não existe |
| R4 | Operações **stateless** (filter, map) e **stateful** (join, aggregate, janelas) da DSL | Nenhuma. O `pom.xml` só tem `kafka-clients`, sem `kafka-streams` |
| R5 | Arquitetura Web (back-end + front-end) com visualização amigável | Já existe em parte: `Notificador` com HttpServer e `painel.html` |
| R6 | Continuidade total da etapa 1 | F1 a F3, os produtores e o replay continuam valendo |
| — | Entrega em **07/12/2026**, com apresentação em aula | — |

## 2. Problemas de base para resolver antes de qualquer topologia

São esses os pontos que derrubam um projeto de Streams quando ficam para depois.

1. **Dependências.** Falta `org.apache.kafka:kafka-streams:4.2.0` (mesma versão dos clients). Para testes, faltam `kafka-streams-test-utils` e JUnit.
2. **Serde.** O Streams pede `Serde<T>`. Dá para criar um `JsonSerde<T>` em `serde/` reaproveitando o `JsonSerializer`/`JsonDeserializer` que já existem.
3. **Tempo do evento: é o ponto mais crítico.** Nenhum produtor define o timestamp do `ProducerRecord`, então cada registro fica com a hora do envio. No `ProdutorReplay`, 48 horas de dados são enviadas em segundos, e todas as janelas do Streams acabariam caindo no mesmo intervalo. É preciso:
   - um `TimestampExtractor` que leia `dataHoraMedicao` (Medicao) e `dataHora` (CondicaoClima), convertendo para `America/Sao_Paulo`;
   - ou então gravar o timestamp nos próprios produtores. O ideal é fazer as duas coisas.

   O `DetectorSensor` já segue esse raciocínio ("tempo do stream, não do relógio"), o que é bom para a narrativa da apresentação.
4. **Configuração das aplicações Streams.** Cada uma precisa de:
   - `application.id` próprio;
   - `state.dir` dentro de `saida/` (o padrão `/tmp` é ruim no Windows);
   - `replication.factor=3` para os tópicos internos (changelog e repartition);
   - opcionalmente `processing.guarantee=exactly_once_v2`, que o cluster de 3 brokers suporta e vale como ponto extra.
5. **Co-particionamento.** `ar.medicoes` tem 3 partições com chave `idEstacao`. `clima.condicoes` tem 1 partição com chave fixa. Um join `KStream-KTable` direto não funciona. Há duas saídas:
   - **GlobalKTable** de clima com chave "hora" (por exemplo `2026-09-20T03`), e cada medição consulta a hora correspondente. É a opção mais simples e robusta.
   - **flatMap** do clima para as 8 estações, repartition e um join `KStream-KStream` com `JoinWindows` de cerca de 1 hora. É mais trabalhoso, mas mostra join com janela, o que rende bem na apresentação.
6. **Infraestrutura.** Os tópicos novos precisam entrar no serviço `criar-topicos` do `docker-compose.yml` e em `Topicos.java`. O enum `Alerta.Tipo` precisa dos novos tipos (F5, F6, F7…).

## 3. R1: F4 em Kafka Streams

Proposta de topologia, aproveitando a mesma semântica de hoje:

```
ar.medicoes ──filter(operacional)──► join(GlobalKTable clima por hora) ──► MedicaoEnriquecida
   └─► groupByKey ─► aggregate(SequenciaEstagnacao)       ← stateful, store RocksDB
         estado: horas consecutivas > 40, vento acumulado, chuva acumulada, já anunciado?
   └─► toStream ─► filter(atingiu 6h && vento médio < 8 && chuva == 0 && !anunciado)
   └─► mapValues(EventoEstagnacao) ─► to(ar.eventos-derivados)
```

- Um `aggregate` por estação, com uma pequena máquina de estados, resolve três coisas de uma vez: a exigência de horas **consecutivas**, o "um evento por episódio" (que hoje é o `Set emEpisodio`) e o reinício quando o IQAr cai.
- Uma alternativa é janela *hopping* de 6h com avanço de 1h e `suppress(untilWindowCloses)`. Para "consecutivas", porém, a agregação com estado é mais fiel.
- O `AlertaEstagnacao` atual pode continuar lendo `ar.eventos-derivados`, ou virar um `mapValues` dentro da própria topologia.
- O formato do `EventoEstagnacao` fica o mesmo. O painel e o CSV não precisam mudar.

## 4. R2 e R3: sugestões de 3 novas situações

Escolhi situações que têm sentido no domínio (qualidade do ar na Grande Vitória) e que, juntas, cobrem todos os operadores pedidos.

**F5. Propagação de pluma entre estações (Allen)**
- Cada estação gera intervalos de poluição, montados com `SessionWindows` (gap de cerca de 90 min) sobre `filter(IQAr > 40)`. Cada sessão vira um `Intervalo[início, fim]`.
- Os intervalos de estações vizinhas são combinados (por exemplo Jardim Camburi e Enseada do Suá; Vitória Centro e IBES).
- A relação é classificada com Allen: **BEFORE / MEETS / OVERLAPS** com defasagem coerente com a direção do vento indica uma pluma se deslocando.
- Operadores: session window, groupBy/repartition, join, mapValues.

**F6. Lacuna crítica de monitoramento (Allen, combina com F3)**
- O intervalo de estação offline (vindo de F3) é cruzado com o intervalo de episódio em estações vizinhas.
- Se o offline **OVERLAPS**, **DURING** ou **CONTAINS** o episódio, há uma estação muda justamente durante a poluição regional, e o alerta é de severidade ALTA.
- Dá continuidade direta à narrativa da etapa 1 ("silêncio não é ausência de poluição").

**F7. Episódio regional simultâneo (agregação com janela, sem Allen)**
- Janela *tumbling* de 1h: `groupBy(região ou município)` seguido de `count`/`aggregate` das estações em faixa ≥ Moderada.
- Se 3 ou mais estações estão nessa situação na mesma hora, o problema é regional e não local.
- Operadores: `selectKey`/`groupBy` (repartition), `TimeWindows`, `aggregate`, `suppress`.

Alternativas, se alguma acima não convencer:
- **Lavagem pela chuva**: um intervalo de chuva **MEETS** ou **OVERLAPS** a queda do IQAr. É um evento "positivo", que fica bonito no painel.
- **Pó preto**: particulado alto em Jardim Camburi **DURING** um intervalo de vento vindo do quadrante NE/E (região do porto de Tubarão).
- **Média móvel de 24h da concentração** comparada ao padrão CONAMA. Precisaria conferir o limite vigente na resolução.

**Implementação de Allen:** um record `Intervalo` e um enum `RelacaoAllen` com as 13 relações (`before`, `meets`, `overlaps`, `starts`, `during`, `finishes`, `equals` e as inversas), mais `Allen.relacao(a, b)` e testes unitários. Deixe isso isolado e bem testado: é o item que o professor vai olhar com mais atenção.

## 5. R4: cobertura dos operadores da DSL

| Operador | Onde aparece |
|---|---|
| `filter`, `filterNot` | medições operacionais (F4, F5, F7) |
| `map`, `mapValues`, `selectKey`, `flatMap` | enriquecimento, troca de chave por região, montagem dos alertas |
| `join` (stream-GlobalKTable e/ou stream-stream com janela) | F4 (clima), F5 e F6 (intervalos) |
| `aggregate`, `count`, `reduce` | F4 (sequência), F7 (contagem regional) |
| `TimeWindows` / `SessionWindows` / hopping | F7 / F5 e F6 / alternativa de F4 |
| `suppress` | resultado final das janelas |

## 6. R5: Web e front-end

O painel atual já atende o básico. Ideias para a parte de "criatividade":
- **Interactive Queries.** Expor os state stores do Streams via HTTP (por exemplo `/api/estacoes/{id}/sequencia` ou `/api/regiao/agora`). Isso mostra que o estado vive no Streams, não em `HashMap`s.
- **Linha do tempo de intervalos** no estilo Gantt, por estação, destacando as relações de Allen detectadas. É a melhor visualização para explicar Allen na apresentação.
- Gráfico de série temporal do IQAr por estação.
- Novos tipos de alerta e cores no painel e no CSV.

## 7. Qualidade e apresentação

- **Testes com `TopologyTestDriver`**: dá para testar cada topologia sem cluster, controlando o tempo do evento. Também ajuda a mostrar Allen com cenários construídos à mão.
- **Replay**: o `ProdutorReplay` já reenvia o clima histórico. Com o `TimestampExtractor` do item 2.3, as janelas passam a funcionar no replay acelerado.
- **README**: novo diagrama com as topologias (vale usar `topology.describe()`), tabela de situações F5 a F7 e explicação de Allen.

## 8. Plano de execução sugerido

1. **Base:** dependência do Streams, `JsonSerde`, `TimestampExtractor`, helper de configuração (como o `Kafka.java`), tópicos novos, ajuste dos produtores para gravar o timestamp.
2. **F4 em Streams**, com teste no `TopologyTestDriver`, conferindo no replay que o resultado bate com o F4 antigo (o episódio da Enseada do Suá de 20/09).
3. **Utilitário de Allen** e seus testes.
4. **F5, F6 e F7**, cada um como uma classe com `application.id` próprio.
5. **Notificador e painel:** novos tipos de alerta, Interactive Queries, timeline.
6. **README, diagramas e roteiro da apresentação.**

## 9. Decisões tomadas (13/11/2026)

| # | Decisão | Escolha | Consequência |
|---|---|---|---|
| 1 | Novas situações de interesse | **F5** (propagação de pluma, Allen) + **F6** (lacuna crítica de monitoramento, Allen) + **F7** (episódio regional, janela de 1h) | Allen aparece em duas situações; F7 cobre janela fixa e agregação |
| 2 | Join do clima na F4 | **GlobalKTable** indexada pela hora cheia | Clima rechaveado para o tópico compactado `clima.por-hora`; cada medição consulta a sua hora, sem repartition |
| 3 | F1 a F3 | **Continuam como estão** (consumers da etapa 1) | F6 monta seus próprios intervalos de "estação offline" a partir de `ar.medicoes` (session window), sem depender de F3 |
| 4 | `CorrelacionadorEstagnacao` (F4 antigo) | **Removido e substituído** pela versão Streams | Mesmo `EventoEstagnacao` em `ar.eventos-derivados`; `AlertaEstagnacao`, painel e CSV não mudam. A versão antiga fica no histórico do git |

## 10. Andamento

### Etapa 1: base do Kafka Streams (concluída, ainda não executada contra o cluster)

- `pom.xml`: `kafka-streams` 4.2.0, `kafka-streams-test-utils`, JUnit 5 e surefire. A `jackson-bom` foi importada porque o `kafka-streams` trazia um `jackson-annotations` 2.19 que quebrava o `ObjectMapper`.
- `infra/TempoDoEvento`: converte `dataHoraMedicao`/`dataHora` (horário de São Paulo) em timestamp. Ignora a data sentinela `0001-01-01` e não grava timestamps no futuro.
- Produtores (`ProdutorIema`, `ProdutorClima`, `ProdutorReplay`): passam a gravar o **tempo do evento** no `ProducerRecord`.
- `streams/ExtratorTempoEvento`: `TimestampExtractor` que lê o tempo de dentro do evento. A leitura de estação offline entra no tempo corrente da partição. Se a partição ainda não tem tempo, a leitura é descartada, para não avançar o relógio do replay.
- `streams/Streams`: configuração comum (`application.id`, extrator, `exactly_once_v2`, replicação 3, `state.dir` em `saida/`, `LogAndContinue`) e ciclo de vida (`describe()`, shutdown hook). `-Dapp.novo=1` reprocessa do início.
- `serde/JsonSerde`: Serde para o Streams sobre o serializador e o desserializador da etapa 1.
- `docker-compose.yml` e `Topicos`: tópicos `clima.por-hora` (1 partição, compactado) e `ar.intervalos` (3 partições).
- `config.properties`: `tempo.zona`, `streams.state.dir`, `streams.replication.factor`, `streams.processing.guarantee`.
- Testes (`mvn package`): 12 testes passando, incluindo uma topologia no `TopologyTestDriver` que mostra janelas montadas pela hora da medição, mesmo com todos os registros chegando no mesmo instante.

**Observação já existente na etapa 1:** o `kafka-clients` traz `slf4j-api` 1.7, que não reconhece o `slf4j-simple` 2.0. Por isso os logs da biblioteca Kafka saem desligados (NOP). Corrigir isso deixa o console bem mais verboso, então fica como decisão à parte.

### Próximas etapas

2. F4 em Kafka Streams (GlobalKTable de clima + agregação de sequência por estação), com teste, e remoção do `CorrelacionadorEstagnacao`.
3. Utilitário da álgebra de Allen (`Intervalo`, `RelacaoAllen` com as 13 relações) e testes.
4. F5, F6 e F7.
5. Notificador e painel: novos tipos de alerta, Interactive Queries, linha do tempo de intervalos.
6. README, diagramas e roteiro da apresentação.
