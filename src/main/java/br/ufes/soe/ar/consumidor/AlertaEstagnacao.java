package br.ufes.soe.ar.consumidor;

import br.ufes.soe.ar.infra.Topicos;
import br.ufes.soe.ar.modelo.Alerta;
import br.ufes.soe.ar.modelo.EventoEstagnacao;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.time.Duration;
import java.time.Instant;

/**
 * Consome o evento DERIVADO de {@code ar.eventos-derivados} e emite o alerta de
 * severidade ALTA correspondente em {@code ar.alertas}.
 *
 * <p>Existe separado do correlacionador de proposito. E ele que fecha a cadeia
 * completa que o enunciado descreve:
 *
 * <pre>
 *   evento primitivo  ->  correlacionador  ->  evento derivado  ->  acao
 *   (ar.medicoes)         (infere)             (ar.eventos-           (ar.alertas)
 *   (clima.condicoes)                           derivados)
 * </pre>
 *
 * <p>O enunciado diz que os eventos derivados "podem ser consumidos para tomada de
 * decisoes/acoes". Este componente e essa tomada de acao — e demonstra que o evento
 * derivado e um cidadao de primeira classe no Kafka, nao uma variavel interna do
 * correlacionador.
 */
public class AlertaEstagnacao {

    private static final String GRUPO = "alerta-estagnacao";

    public static void main(String[] args) {
        System.out.printf("%s | consome %s -> publica em %s%n",
                GRUPO, Topicos.DERIVADOS, Topicos.ALERTAS);

        try (KafkaConsumer<String, EventoEstagnacao> consumidor =
                     Kafka.consumidor(GRUPO, EventoEstagnacao.class, Topicos.DERIVADOS);
             KafkaProducer<String, Alerta> produtor = Kafka.produtor()) {

            while (true) {
                for (ConsumerRecord<String, EventoEstagnacao> r
                        : consumidor.poll(Duration.ofMillis(1000))) {

                    EventoEstagnacao e = r.value();
                    if (e == null) {
                        continue;
                    }

                    Kafka.publicarAlerta(produtor, new Alerta(
                            Alerta.Tipo.F4_ESTAGNACAO,
                            Alerta.Severidade.ALTA,
                            e.idEstacao(),
                            e.localizacao(),
                            e.municipio(),
                            e.poluenteCritico(),
                            e.iqarMaximo(),
                            "Moderada ou pior",
                            ("Episodio de estagnacao atmosferica: %s. "
                                    + "Recomenda-se restringir atividades ao ar livre na regiao.")
                                    .formatted(e.descricaoCurta()),
                            Instant.now()));
                }
            }
        }
    }
}
