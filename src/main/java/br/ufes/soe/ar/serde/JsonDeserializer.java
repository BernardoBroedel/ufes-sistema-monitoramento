package br.ufes.soe.ar.serde;

import org.apache.kafka.common.serialization.Deserializer;

/**
 * Desserializador generico, contraparte do {@link JsonSerializer}.
 *
 * <p>Recebe a classe alvo pelo construtor e e passado direto ao KafkaConsumer:
 * <pre>
 *   new KafkaConsumer&lt;&gt;(props, new StringDeserializer(),
 *                           new JsonDeserializer&lt;&gt;(Medicao.class));
 * </pre>
 *
 * <p>Uma mensagem mal formada nao derruba o consumidor: ela e registrada e
 * descartada, e o laco segue. Num sistema de monitoramento, parar de monitorar
 * por causa de um evento corrompido seria pior que perder aquele evento.
 */
public class JsonDeserializer<T> implements Deserializer<T> {

    private final Class<T> tipo;

    public JsonDeserializer(Class<T> tipo) {
        this.tipo = tipo;
    }

    @Override
    public T deserialize(String topic, byte[] dados) {
        if (dados == null || dados.length == 0) {
            return null;
        }
        try {
            return Json.mapper().readValue(dados, tipo);
        } catch (Exception e) {
            System.err.printf("[%s] evento descartado, JSON invalido para %s: %s%n",
                    topic, tipo.getSimpleName(), e.getMessage());
            return null;
        }
    }
}
