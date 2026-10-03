package br.ufes.soe.ar.produtor;

import br.ufes.soe.ar.infra.ClienteOpenMeteo;
import br.ufes.soe.ar.infra.Config;
import br.ufes.soe.ar.infra.Topicos;
import br.ufes.soe.ar.modelo.CondicaoClima;
import br.ufes.soe.ar.serde.JsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.format.DateTimeFormatter;
import java.util.Properties;

/**
 * SEGUNDO PRODUTOR. Publica vento e chuva da Grande Vitoria em
 * {@code clima.condicoes}, a cada quinze minutos.
 *
 * <p>A chave e fixa porque existe uma unica serie temporal global. O topico tem
 * uma particao so, o que garante ordem total dos eventos de clima — importante
 * para a janela deslizante de F4.
 *
 * <p>Uso: {@code java -cp target/monitoramento-ar.jar br.ufes.soe.ar.produtor.ProdutorClima}
 */
public class ProdutorClima {

    private static final String CHAVE = "grande-vitoria";
    private static final DateTimeFormatter HORA = DateTimeFormatter.ofPattern("HH:mm:ss");

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.bootstrap());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);

        ClienteOpenMeteo clima = new ClienteOpenMeteo();
        Duration intervalo = Duration.ofSeconds(Config.inteiro("clima.intervalo.segundos"));
        LocalDateTime ultima = null;

        System.out.println("produtor-clima | topico=" + Topicos.CLIMA
                + " | polling a cada " + intervalo.toSeconds() + "s");
        System.out.println("Ctrl+C para encerrar.\n");

        try (KafkaProducer<String, CondicaoClima> produtor = new KafkaProducer<>(props)) {

            while (true) {
                try {
                    CondicaoClima c = clima.lerCondicaoAtual();

                    // O Open-Meteo atualiza de 15 em 15 minutos; se a leitura tem o
                    // mesmo carimbo da anterior, nao ha evento novo a publicar.
                    if (c.dataHora().equals(ultima)) {
                        System.out.printf("[%s] sem leitura nova (%s)%n",
                                LocalTime.now().format(HORA), c.dataHora());
                    } else {
                        produtor.send(new ProducerRecord<>(Topicos.CLIMA, CHAVE, c));
                        produtor.flush();
                        ultima = c.dataHora();
                        System.out.printf("[%s] publicado %s | %s%n",
                                LocalTime.now().format(HORA), c.dataHora(), c.descricaoCurta());
                    }

                } catch (Exception e) {
                    System.err.printf("[%s] falha na coleta de clima: %s%n",
                            LocalTime.now().format(HORA), e.getMessage());
                }

                Thread.sleep(intervalo.toMillis());
            }

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
