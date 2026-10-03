package br.ufes.soe.ar.serde;

import org.apache.kafka.common.serialization.Serializer;

/**
 * Serializador generico para os eventos do projeto.
 *
 * <p>Mesmo padrao do {@code StudentSerializer} do Lab2 (ObjectMapper do Jackson
 * convertendo o objeto em bytes), porem parametrizado — um unico SerDes atende
 * Medicao, CondicaoClima, Alerta e EventoEstagnacao.
 *
 * <p>Lembrando a Aula 4: o Kafka so trafega sequencia de bytes. Quem da significado
 * a esses bytes e o par serializador/desserializador.
 */
public class JsonSerializer<T> implements Serializer<T> {

    @Override
    public byte[] serialize(String topic, T dado) {
        if (dado == null) {
            return null;
        }
        try {
            return Json.mapper().writeValueAsBytes(dado);
        } catch (Exception e) {
            throw new RuntimeException("Falha ao serializar para o topico " + topic, e);
        }
    }
}
