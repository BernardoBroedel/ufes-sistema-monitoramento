package br.ufes.soe.ar.modelo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;

/**
 * EVENTO PRIMITIVO do sistema: uma leitura horaria de uma estacao da rede do IEMA.
 *
 * <p>Publicado no topico {@code ar.medicoes}, chaveado por {@code idEstacao} — assim
 * todas as leituras de uma mesma estacao caem na mesma particao e preservam ordem,
 * que e o que as regras de janela temporal (F1 e F4) exigem.
 *
 * <p>Campos podem vir nulos quando a estacao esta fora de operacao. E justamente
 * esse caso que a funcionalidade F3 detecta.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Medicao(
        int idEstacao,
        String estacao,
        String localizacao,
        String municipio,
        double lat,
        double lon,
        String poluenteCritico,
        Double valor,
        double iqar,
        String faixa,
        LocalDateTime dataHoraMedicao,
        Instant dataHoraColeta
) {

    /** Ano sentinela que a API usa para indicar ausencia de medicao. */
    private static final int ANO_INVALIDO = 1;

    /** Faixa classificada, derivada do texto devolvido pela API. */
    @JsonIgnore
    public Faixa faixaClassificada() {
        return Faixa.de(faixa);
    }

    /**
     * true quando a estacao entregou uma leitura utilizavel.
     *
     * <p>Uma estacao offline se manifesta de tres formas na API, e todas ocorrem de
     * verdade: poluente nulo, data sentinela {@code 0001-01-01} e IQAr zerado.
     */
    @JsonIgnore
    public boolean operacional() {
        return poluenteCritico != null
                && dataHoraMedicao != null
                && dataHoraMedicao.getYear() > ANO_INVALIDO
                && iqar > 0;
    }

    /** true se o poluente critico e material particulado (MP10 ou MP2,5). */
    @JsonIgnore
    public boolean ehParticulado() {
        if (poluenteCritico == null) {
            return false;
        }
        String p = poluenteCritico.toLowerCase();
        return p.contains("part") || p.contains("mp");
    }

    /** Ha quanto tempo esta medicao foi registrada pela estacao. */
    @JsonIgnore
    public Duration atraso(LocalDateTime agora) {
        if (dataHoraMedicao == null || dataHoraMedicao.getYear() <= ANO_INVALIDO) {
            return Duration.ofDays(999);
        }
        return Duration.between(dataHoraMedicao, agora);
    }

    /** Identidade da leitura, usada para deduplicar o polling de 5 em 5 minutos. */
    @JsonIgnore
    public String chaveDeduplicacao() {
        return idEstacao + "@" + dataHoraMedicao;
    }

    @JsonIgnore
    public String descricaoCurta() {
        return "%s (%s) IQAr %.1f [%s] %s"
                .formatted(localizacao, municipio, iqar, faixa, poluenteCritico);
    }
}
