package br.ufes.soe.ar.infra;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * Conversao entre a data de um evento (como o IEMA e o Open-Meteo a devolvem) e o
 * timestamp do registro no Kafka.
 *
 * <p><b>Por que isto importa no Trabalho 2.</b> As janelas, joins e sessoes do
 * Kafka Streams sao todos calculados sobre o timestamp do registro. Se o produtor
 * nao o define, o Kafka grava a hora do envio, e entao:
 * <ul>
 *   <li>no replay, 48 horas de historico enviadas em poucos segundos cairiam todas
 *       na mesma janela;</li>
 *   <li>ao vivo, a leitura das 14h publicada as 14h02 ficaria registrada como 14h02.</li>
 * </ul>
 * O sistema raciocina sobre eventos, entao a referencia de tempo tem que vir dos
 * proprios eventos, e nao do relogio de quem publicou.
 */
public final class TempoDoEvento {

    /** Ano sentinela que a API do IEMA usa para "sem medicao" (0001-01-01). */
    private static final int ANO_INVALIDO = 1;

    /**
     * Folga para relogios ligeiramente adiantados. Acima disso o timestamp e
     * descartado no produtor: o broker pode rejeitar registros muito no futuro
     * (configuracao {@code log.message.timestamp.after.max.ms}).
     */
    private static final long FOLGA_FUTURO_MS = 5 * 60 * 1000L;

    private static final ZoneId ZONA = ZoneId.of(Config.texto("tempo.zona"));

    private TempoDoEvento() {
    }

    public static ZoneId zona() {
        return ZONA;
    }

    /**
     * Instante do evento em milissegundos, ou {@code null} se a data esta ausente
     * ou e a sentinela de estacao offline.
     */
    public static Long epochMillis(LocalDateTime dataHora) {
        if (dataHora == null || dataHora.getYear() <= ANO_INVALIDO) {
            return null;
        }
        return dataHora.atZone(ZONA).toInstant().toEpochMilli();
    }

    /** Operacao inversa, para montar eventos derivados a partir de janelas. */
    public static LocalDateTime deEpochMillis(long epochMillis) {
        return LocalDateTime.ofInstant(Instant.ofEpochMilli(epochMillis), ZONA);
    }

    /**
     * Timestamp para o {@code ProducerRecord}. Devolve {@code null} quando nao ha
     * data valida ou quando ela esta no futuro: nesses casos o produtor usa a hora
     * do envio, que e o comportamento antigo.
     */
    public static Long paraRegistro(LocalDateTime dataHora) {
        Long t = epochMillis(dataHora);
        if (t == null || t > System.currentTimeMillis() + FOLGA_FUTURO_MS) {
            return null;
        }
        return t;
    }
}
