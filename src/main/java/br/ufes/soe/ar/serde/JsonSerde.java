package br.ufes.soe.ar.serde;

import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;

/**
 * Serde JSON para o Kafka Streams: junta o {@link JsonSerializer} e o
 * {@link JsonDeserializer} que os produtores e consumidores da etapa 1 ja usam.
 *
 * <p>O Kafka Streams le e escreve topicos (e state stores) por conta propria,
 * entao precisa do par serializador/desserializador numa peca so:
 * <pre>
 *   builder.stream(Topicos.MEDICOES, Consumed.with(Serdes.String(), JsonSerde.de(Medicao.class)))
 * </pre>
 *
 * <p>Herda o comportamento do desserializador: JSON invalido vira {@code null} e e
 * registrado no console, em vez de derrubar a aplicacao. Por isso as topologias
 * devem filtrar valores nulos logo na entrada.
 */
public final class JsonSerde<T> implements Serde<T> {

    private final JsonSerializer<T> serializador = new JsonSerializer<>();
    private final JsonDeserializer<T> desserializador;

    private JsonSerde(Class<T> tipo) {
        this.desserializador = new JsonDeserializer<>(tipo);
    }

    public static <T> JsonSerde<T> de(Class<T> tipo) {
        return new JsonSerde<>(tipo);
    }

    @Override
    public Serializer<T> serializer() {
        return serializador;
    }

    @Override
    public Deserializer<T> deserializer() {
        return desserializador;
    }
}
