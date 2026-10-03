package br.ufes.soe.ar.modelo;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.Locale;

/**
 * Saida unificada das quatro funcionalidades. Publicado em {@code ar.alertas},
 * chaveado pelo {@code tipo}, e consumido pelo notificador (CSV + painel HTTP).
 */
@JsonIgnoreProperties(ignoreUnknown = true)
public record Alerta(
        Tipo tipo,
        Severidade severidade,
        int idEstacao,
        String localizacao,
        String municipio,
        String poluente,
        double iqar,
        String faixa,
        String mensagem,
        Instant dataHora
) {

    /** Uma constante por funcionalidade descrita na secao 7 da proposta. */
    public enum Tipo {
        F1_SUBIDA_ACELERADA,
        F2_MUDANCA_FAIXA,
        F3_ESTACAO_OFFLINE,
        F4_ESTAGNACAO
    }

    public enum Severidade {
        BAIXA, MEDIA, ALTA
    }

    /**
     * Linha do arquivo CSV (funcionalidade desejavel 10.1 da proposta).
     *
     * <p>Separador ponto e virgula e decimal com virgula: e a convencao brasileira,
     * e faz o Excel em pt-BR abrir o arquivo com um duplo clique, sem assistente de
     * importacao. Usar virgula como separador junto com o locale pt-BR quebraria o
     * arquivo — um IQAr de 20,91 viraria duas colunas.
     */
    @JsonIgnore
    public String paraCsv() {
        return String.join(";",
                HORARIO.format(dataHora),
                tipo.name(),
                severidade.name(),
                String.valueOf(idEstacao),
                escapar(localizacao),
                escapar(municipio),
                escapar(poluente),
                String.format(BRASIL, "%.2f", iqar),
                escapar(faixa),
                escapar(mensagem));
    }

    public static String cabecalhoCsv() {
        return "dataHora;tipo;severidade;idEstacao;localizacao;municipio;"
                + "poluente;iqar;faixa;mensagem";
    }

    private static final Locale BRASIL = Locale.of("pt", "BR");

    /** Horario local, e nao o Instant em UTC: quem le a planilha esta em Vitoria. */
    private static final DateTimeFormatter HORARIO =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")
                    .withZone(ZoneId.systemDefault());

    /** Envolve em aspas e duplica aspas internas, conforme RFC 4180. */
    private static String escapar(String s) {
        if (s == null) {
            return "";
        }
        return '"' + s.replace("\"", "\"\"") + '"';
    }
}
