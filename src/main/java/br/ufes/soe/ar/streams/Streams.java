package br.ufes.soe.ar.streams;

import br.ufes.soe.ar.infra.Config;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.errors.LogAndContinueExceptionHandler;
import org.apache.kafka.streams.errors.StreamsUncaughtExceptionHandler;

import java.time.Duration;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Configuracao e ciclo de vida comuns a todas as aplicacoes Kafka Streams do
 * projeto. E o equivalente, no Trabalho 2, da classe {@code Kafka} da etapa 1.
 *
 * <p>Cada situacao de interesse e uma aplicacao Streams independente, com seu
 * proprio {@code application.id}. O {@code application.id} faz tres papeis ao mesmo
 * tempo: e o consumer group, o prefixo dos topicos internos (changelog e
 * repartition) e o nome da pasta de estado local.
 */
public final class Streams {

    private Streams() {
    }

    /**
     * Propriedades padrao de uma aplicacao Streams.
     *
     * <p>{@code -Dapp.novo=1} cria um {@code application.id} inedito, que portanto
     * reprocessa os topicos desde o inicio. E o mesmo papel do {@code -Dgrupo.novo=1}
     * dos consumidores da etapa 1.
     */
    public static Properties configuracao(String applicationId) {
        if (System.getProperty("app.novo") != null) {
            applicationId = applicationId + "-" + System.currentTimeMillis();
        }

        Properties p = new Properties();
        p.put(StreamsConfig.APPLICATION_ID_CONFIG, applicationId);
        p.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Config.bootstrap());

        // Chaves sao sempre texto (idEstacao, hora, regiao). Os valores tem Serde
        // explicito em cada operacao, via JsonSerde.de(Classe.class).
        p.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class);

        // Janelas e joins sobre o tempo do evento, nao sobre o tempo de chegada.
        p.put(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, ExtratorTempoEvento.class);

        // Evento corrompido e registrado e pulado, como nos consumidores da etapa 1.
        p.put(StreamsConfig.DESERIALIZATION_EXCEPTION_HANDLER_CLASS_CONFIG,
                LogAndContinueExceptionHandler.class);

        p.put(StreamsConfig.REPLICATION_FACTOR_CONFIG, Config.inteiro("streams.replication.factor"));
        p.put(StreamsConfig.STATE_DIR_CONFIG, Config.texto("streams.state.dir"));
        p.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, Config.texto("streams.processing.guarantee"));
        return p;
    }

    /**
     * Imprime a topologia, sobe a aplicacao e bloqueia ate Ctrl+C.
     *
     * <p>A descricao impressa ({@code topology.describe()}) mostra cada no da DSL:
     * fontes, filtros, joins, stores e sumidouros. Serve de diagrama na apresentacao.
     */
    public static void executar(Topology topologia, Properties props) {
        System.out.println(topologia.describe());

        KafkaStreams app = new KafkaStreams(topologia, props);
        CountDownLatch fim = new CountDownLatch(1);

        // Uma excecao inesperada substitui so a thread afetada, e o restante da
        // aplicacao continua monitorando.
        app.setUncaughtExceptionHandler(e -> {
            System.err.println("erro na thread de processamento: " + e);
            return StreamsUncaughtExceptionHandler.StreamThreadExceptionResponse.REPLACE_THREAD;
        });
        app.setStateListener((novo, antigo) ->
                System.out.printf("  (estado: %s -> %s)%n", antigo, novo));

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("\nencerrando " + props.get(StreamsConfig.APPLICATION_ID_CONFIG) + "...");
            app.close(Duration.ofSeconds(10));
            fim.countDown();
        }, "encerramento"));

        app.start();
        System.out.println(props.get(StreamsConfig.APPLICATION_ID_CONFIG)
                + " no ar. Ctrl+C para encerrar.\n");
        try {
            fim.await();
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
