package br.ufes.soe.ar.streams;

import br.ufes.soe.ar.infra.TempoDoEvento;
import br.ufes.soe.ar.modelo.CondicaoClima;
import br.ufes.soe.ar.modelo.Medicao;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;

/**
 * Ensina o Kafka Streams a usar o TEMPO DO EVENTO, e nao o tempo de chegada.
 *
 * <p>Toda janela, join e sessao das topologias e calculada sobre o valor que este
 * extrator devolve. Ele le a data de dentro do proprio evento:
 * <ul>
 *   <li>{@link Medicao}: {@code dataHoraMedicao}, a hora em que a estacao mediu;</li>
 *   <li>{@link CondicaoClima}: {@code dataHora}, a hora da condicao meteorologica;</li>
 *   <li>qualquer outro valor: o timestamp do registro. Eventos produzidos pelas
 *       proprias topologias ja herdam o tempo do evento de origem.</li>
 * </ul>
 *
 * <p><b>Estacao offline.</b> A leitura de uma estacao fora do ar vem com a data
 * sentinela {@code 0001-01-01}, que nao e tempo nenhum. Ela e posicionada no tempo
 * corrente da particao ({@code partitionTime}), ou seja, "agora, na linha do tempo
 * dos eventos". Se a particao ainda nao viu nenhum evento valido, nao ha onde
 * encaixa-la: o extrator devolve um valor negativo e o Streams descarta o registro.
 * Usar o relogio da maquina nesse caso seria pior: no replay, um unico registro
 * com a hora de hoje avancaria o tempo do stream em dois dias e fecharia todas as
 * janelas do historico.
 *
 * <p>Os produtores tambem gravam o tempo do evento no registro (ver
 * {@link TempoDoEvento#paraRegistro}). O extrator e a garantia do lado de quem le:
 * funciona mesmo com dados publicados antes dessa mudanca.
 */
public class ExtratorTempoEvento implements TimestampExtractor {

    @Override
    public long extract(ConsumerRecord<Object, Object> registro, long partitionTime) {
        Object valor = registro.value();

        if (valor instanceof Medicao m) {
            Long t = TempoDoEvento.epochMillis(m.dataHoraMedicao());
            return t != null ? t : partitionTime;
        }
        if (valor instanceof CondicaoClima c) {
            Long t = TempoDoEvento.epochMillis(c.dataHora());
            return t != null ? t : partitionTime;
        }
        return registro.timestamp();
    }
}
