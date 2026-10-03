package br.ufes.soe.ar.produtor;

import br.ufes.soe.ar.infra.ClienteIema;
import br.ufes.soe.ar.infra.Config;
import br.ufes.soe.ar.infra.RegistroDeduplicacao;
import br.ufes.soe.ar.infra.Topicos;
import br.ufes.soe.ar.modelo.Medicao;
import br.ufes.soe.ar.serde.JsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Properties;

/**
 * PRODUTOR PRINCIPAL. Faz polling da API do IEMA e publica cada leitura nova como
 * evento primitivo em {@code ar.medicoes}.
 *
 * <p>A chave da mensagem e o {@code idEstacao}. Isso aplica a particao semantica
 * discutida na Aula 4: todas as leituras de uma estacao caem sempre na mesma
 * particao e, portanto, chegam em ordem ao consumidor. As regras de janela
 * temporal de F1 e F4 dependem disso.
 *
 * <p><b>Deduplicacao.</b> A API atualiza de hora em hora, mas o polling e de cinco
 * em cinco minutos para reduzir a latencia de deteccao. Sem filtro, cada estacao
 * geraria onze eventos repetidos por hora. Por isso so publicamos quando o par
 * {@code (idEstacao, dataHoraMedicao)} ainda nao foi visto.
 *
 * <p>Uso: {@code java -cp target/monitoramento-ar.jar br.ufes.soe.ar.produtor.ProdutorIema}
 */
public class ProdutorIema {

    private static final DateTimeFormatter HORA = DateTimeFormatter.ofPattern("HH:mm:ss");

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.bootstrap());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        // Espera confirmacao de todas as replicas em sincronia: com replicacao 3,
        // um evento so e dado como publicado quando sobrevive a queda de um broker.
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        ClienteIema iema = new ClienteIema();
        Duration intervalo = Duration.ofSeconds(Config.inteiro("iema.intervalo.segundos"));
        RegistroDeduplicacao dedup =
                new RegistroDeduplicacao(Config.texto("iema.dedup.arquivo"));

        System.out.println("produtor-iema | topico=" + Topicos.MEDICOES
                + " | bootstrap=" + Config.bootstrap()
                + " | polling a cada " + intervalo.toSeconds() + "s");
        System.out.println("Ctrl+C para encerrar.\n");

        try (KafkaProducer<String, Medicao> produtor = new KafkaProducer<>(props)) {

            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                System.out.println("\nencerrando produtor-iema...");
                produtor.flush();
            }));

            while (true) {
                try {
                    List<Medicao> medicoes = iema.lerEstacoes();
                    int novas = 0;

                    for (Medicao m : medicoes) {
                        if (!dedup.registrarSeNova(m.chaveDeduplicacao())) {
                            continue;   // ja publicada num polling anterior
                        }
                        produtor.send(new ProducerRecord<>(
                                Topicos.MEDICOES, String.valueOf(m.idEstacao()), m));
                        novas++;

                        System.out.printf("  -> [%s] %s%n",
                                m.dataHoraMedicao() == null ? "sem data" : m.dataHoraMedicao(),
                                m.operacional() ? m.descricaoCurta()
                                                : m.localizacao() + " SEM DADOS (estacao offline)");
                    }
                    produtor.flush();
                    dedup.salvar();
                    System.out.printf("[%s] %d estacoes lidas, %d eventos novos publicados%n",
                            java.time.LocalTime.now().format(HORA), medicoes.size(), novas);

                } catch (Exception e) {
                    // A indisponibilidade da API nao pode derrubar o produtor:
                    // ele tenta de novo no proximo ciclo.
                    System.err.printf("[%s] falha na coleta: %s%n",
                            java.time.LocalTime.now().format(HORA), e.getMessage());
                }

                Thread.sleep(intervalo.toMillis());
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
