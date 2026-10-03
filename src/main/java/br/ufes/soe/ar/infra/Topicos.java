package br.ufes.soe.ar.infra;

/** Nomes dos topicos, criados pelo servico {@code criar-topicos} do docker-compose. */
public final class Topicos {

    /** Eventos primitivos: leituras das estacoes. Chave = idEstacao. */
    public static final String MEDICOES = "ar.medicoes";

    /** Eventos primitivos: vento e chuva. Chave fixa, particao unica, ordem total. */
    public static final String CLIMA = "clima.condicoes";

    /** Eventos derivados inferidos pelo correlacionador (F4). */
    public static final String DERIVADOS = "ar.eventos-derivados";

    /** Saida unificada das quatro funcionalidades. Chave = tipo do alerta. */
    public static final String ALERTAS = "ar.alertas";

    private Topicos() {
    }
}
