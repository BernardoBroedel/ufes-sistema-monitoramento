package br.ufes.soe.ar.modelo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.time.LocalDateTime;

/**
 * EVENTO PRIMITIVO da segunda fonte: condicao meteorologica da Grande Vitoria,
 * vinda do Open-Meteo. Publicado no topico {@code clima.condicoes}.
 *
 * <p>Sozinho nao dispara alerta nenhum. Existe para ser correlacionado com as
 * medicoes de ar na funcionalidade F4 (episodio de estagnacao atmosferica).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record CondicaoClima(
        LocalDateTime dataHora,
        double temperatura,
        double precipitacao,
        double ventoVelocidade,
        int ventoDirecao,
        Instant dataHoraColeta
) {

    /** Rosa dos ventos de 16 pontos, para exibicao. */
    private static final String[] ROSA = {
            "N", "NNE", "NE", "ENE", "E", "ESE", "SE", "SSE",
            "S", "SSW", "SW", "WSW", "W", "WNW", "NW", "NNW"
    };

    @JsonIgnore
    public String direcaoCardeal() {
        int i = (int) (((ventoDirecao + 11.25) % 360) / 22.5);
        return ROSA[Math.floorMod(i, ROSA.length)];
    }

    /**
     * true quando o vento esta fraco o bastante para permitir acumulo de poluentes.
     *
     * <p>O limiar vem da analise das 48h reais da rede: o episodio de 33 horas da
     * Enseada do Sua se formou sob vento de 4 a 12 km/h e se dissipou quando o vento
     * virou para NE e passou de 20 km/h.
     */
    @JsonIgnore
    public boolean ventoFraco(double limiarKmH) {
        return ventoVelocidade < limiarKmH;
    }

    /** Chuva remove particulado da atmosfera, entao invalida a hipotese de estagnacao. */
    @JsonIgnore
    public boolean semChuva() {
        return precipitacao <= 0.0;
    }

    @JsonIgnore
    public String descricaoCurta() {
        return "vento %.1f km/h %s | chuva %.1f mm | %.1f C"
                .formatted(ventoVelocidade, direcaoCardeal(), precipitacao, temperatura);
    }
}
