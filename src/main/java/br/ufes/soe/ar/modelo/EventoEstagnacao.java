package br.ufes.soe.ar.modelo;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.time.LocalDateTime;

/**
 * EVENTO DERIVADO (composto) — funcionalidade F4.
 *
 * <p>Publicado em {@code ar.eventos-derivados} pelo correlacionador, que consome
 * eventos primitivos de DUAS fontes ({@code ar.medicoes} e {@code clima.condicoes})
 * e infere um conhecimento que nao existe em nenhum evento isolado.
 *
 * <p>Um episodio de estagnacao so pode ser afirmado pela conjuncao de tres coisas:
 * persistencia temporal (IQAr acima do limite por N horas seguidas), condicao
 * meteorologica (vento fraco) e ausencia de remocao por chuva.
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record EventoEstagnacao(
        String tipo,
        int idEstacao,
        String localizacao,
        String municipio,
        LocalDateTime janelaInicio,
        LocalDateTime janelaFim,
        int horasAcimaDoLimite,
        double iqarMinimo,
        double iqarMaximo,
        String poluenteCritico,
        double ventoMedio,
        String ventoDirecaoPredominante,
        double chuvaAcumulada,
        Instant dataHoraInferencia
) {

    public static final String TIPO = "EPISODIO_ESTAGNACAO";

    public String descricaoCurta() {
        return ("%dh consecutivas acima do limite em %s, IQAr de %.1f a %.1f, "
                + "vento medio %.1f km/h %s, sem chuva")
                .formatted(horasAcimaDoLimite, localizacao, iqarMinimo, iqarMaximo,
                        ventoMedio, ventoDirecaoPredominante);
    }
}
