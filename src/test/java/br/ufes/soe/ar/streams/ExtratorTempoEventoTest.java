package br.ufes.soe.ar.streams;

import br.ufes.soe.ar.Amostras;
import br.ufes.soe.ar.infra.TempoDoEvento;
import br.ufes.soe.ar.modelo.Medicao;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.apache.kafka.common.record.TimestampType;
import org.junit.jupiter.api.Test;

import java.time.LocalDateTime;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ExtratorTempoEventoTest {

    /** Hora em que o registro chegou ao Kafka: "agora", bem depois do evento. */
    private static final long CHEGADA = System.currentTimeMillis();

    private final ExtratorTempoEvento extrator = new ExtratorTempoEvento();

    private static ConsumerRecord<Object, Object> registro(Object valor) {
        return new ConsumerRecord<>("topico", 0, 0L, CHEGADA, TimestampType.CREATE_TIME,
                0, 0, "4", valor, new RecordHeaders(), Optional.empty());
    }

    @Test
    void medicaoUsaAHoraEmQueFoiMedida() {
        LocalDateTime quando = LocalDateTime.of(2026, 9, 20, 3, 0);
        long t = extrator.extract(registro(Amostras.medicao(4, quando, 50)), -1);

        assertEquals(TempoDoEvento.epochMillis(quando), t);
    }

    @Test
    void climaUsaAHoraDaCondicao() {
        LocalDateTime quando = LocalDateTime.of(2026, 9, 20, 3, 15);
        long t = extrator.extract(registro(Amostras.clima(quando, 5, 0)), -1);

        assertEquals(TempoDoEvento.epochMillis(quando), t);
    }

    @Test
    void estacaoOfflineEntraNoTempoCorrenteDaParticao() {
        long tempoDaParticao = TempoDoEvento.epochMillis(LocalDateTime.of(2026, 9, 20, 5, 0));
        Medicao offline = Amostras.offline(2);

        assertEquals(tempoDaParticao, extrator.extract(registro(offline), tempoDaParticao));
    }

    @Test
    void estacaoOfflineSemTempoDeParticaoEDescartada() {
        // Negativo: o Streams descarta, em vez de usar o relogio e avancar o tempo.
        long t = extrator.extract(registro(Amostras.offline(2)), -1);
        assertEquals(-1, t);
    }

    @Test
    void outrosEventosMantemOTimestampDoRegistro() {
        assertEquals(CHEGADA, extrator.extract(registro("qualquer coisa"), -1));
    }
}
