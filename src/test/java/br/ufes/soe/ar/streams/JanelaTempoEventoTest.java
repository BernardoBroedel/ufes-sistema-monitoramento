package br.ufes.soe.ar.streams;

import br.ufes.soe.ar.Amostras;
import br.ufes.soe.ar.infra.TempoDoEvento;
import br.ufes.soe.ar.infra.Topicos;
import br.ufes.soe.ar.modelo.Medicao;
import br.ufes.soe.ar.serde.JsonSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Prova de ponta a ponta da base do Trabalho 2: com a configuracao padrao de
 * {@link Streams}, as janelas sao montadas pela hora da MEDICAO, mesmo quando
 * todos os registros chegam no mesmo instante, como acontece no replay.
 */
class JanelaTempoEventoTest {

    @TempDir
    Path estado;

    @Test
    void janelasSeguemOTempoDoEventoENaoOTempoDeChegada() {
        StreamsBuilder builder = new StreamsBuilder();
        builder.stream(Topicos.MEDICOES, Consumed.with(Serdes.String(), JsonSerde.de(Medicao.class)))
                .groupByKey()
                .windowedBy(TimeWindows.ofSizeWithNoGrace(Duration.ofHours(1)))
                .count()
                .toStream((janela, n) -> janela.key() + "@"
                        + TempoDoEvento.deEpochMillis(janela.window().start()).toLocalTime())
                .to("saida", Produced.with(Serdes.String(), Serdes.Long()));

        Properties props = Streams.configuracao("teste-janela");
        props.put(StreamsConfig.STATE_DIR_CONFIG, estado.toString());

        try (TopologyTestDriver driver = new TopologyTestDriver(builder.build(), props)) {
            TestInputTopic<String, Medicao> entrada = driver.createInputTopic(Topicos.MEDICOES,
                    Serdes.String().serializer(), JsonSerde.de(Medicao.class).serializer());
            TestOutputTopic<String, Long> saida = driver.createOutputTopic("saida",
                    Serdes.String().deserializer(), Serdes.Long().deserializer());

            // Tres horas de historico chegando no MESMO instante, como no replay.
            Instant chegada = Instant.now();
            LocalDateTime base = LocalDateTime.of(2026, 9, 20, 0, 0);
            entrada.pipeInput("4", Amostras.medicao(4, base.withMinute(0), 45), chegada);
            entrada.pipeInput("4", Amostras.medicao(4, base.withMinute(30), 47), chegada);
            entrada.pipeInput("4", Amostras.medicao(4, base.plusHours(1), 50), chegada);
            entrada.pipeInput("4", Amostras.medicao(4, base.plusHours(2), 52), chegada);

            Map<String, Long> ultimaContagem = saida.readKeyValuesToList().stream()
                    .collect(Collectors.toMap(kv -> kv.key, kv -> kv.value, (a, b) -> b));

            // Uma janela por hora de medicao. Com o tempo de chegada, seria uma so,
            // com as quatro leituras.
            assertEquals(Map.of("4@00:00", 2L, "4@01:00", 1L, "4@02:00", 1L), ultimaContagem);
        }
    }
}
