package br.ufes.soe.ar.consumidor;

import br.ufes.soe.ar.infra.Config;
import br.ufes.soe.ar.infra.Topicos;
import br.ufes.soe.ar.modelo.Alerta;
import br.ufes.soe.ar.serde.JsonDeserializer;
import br.ufes.soe.ar.serde.JsonSerializer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.List;
import java.util.Properties;

/** Fabricas de consumidor e produtor, para nao repetir Properties em cada detector. */
public final class Kafka {

    private Kafka() {
    }

    /**
     * Consumidor tipado. Cada detector usa um {@code group.id} proprio, e nao um
     * compartilhado: como discutido na Aula 4, consumidores de grupos diferentes
     * recebem TODOS os eventos do topico, enquanto o mesmo grupo dividiria as
     * particoes entre eles. Aqui cada funcionalidade precisa ver tudo.
     */
    public static <T> KafkaConsumer<String, T> consumidor(String grupo, Class<T> tipo,
                                                          String... topicos) {
        // -Dgrupo.novo=1 cria um consumer group inedito, que portanto comeca do
        // inicio do topico. E o jeito de reprocessar um replay sem precisar mexer
        // nos offsets pelo kafka-consumer-groups.sh.
        if (System.getProperty("grupo.novo") != null) {
            grupo = grupo + "-" + System.currentTimeMillis();
        }
        System.out.println("  (consumer group: " + grupo + ")");

        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.bootstrap());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, grupo);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
                StringDeserializer.class.getName());

        KafkaConsumer<String, T> c = new KafkaConsumer<>(
                props, new StringDeserializer(), new JsonDeserializer<>(tipo));
        c.subscribe(List.of(topicos));
        return c;
    }

    public static <T> KafkaProducer<String, T> produtor() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.bootstrap());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, true);
        return new KafkaProducer<>(props);
    }

    /** Publica um alerta em {@code ar.alertas}, chaveado pelo tipo da situacao. */
    public static void publicarAlerta(KafkaProducer<String, Alerta> produtor, Alerta a) {
        produtor.send(new ProducerRecord<>(Topicos.ALERTAS, a.tipo().name(), a));
        produtor.flush();
        System.out.printf("  !! %s [%s] %s - %s%n",
                a.tipo(), a.severidade(), a.localizacao(), a.mensagem());
    }
}
