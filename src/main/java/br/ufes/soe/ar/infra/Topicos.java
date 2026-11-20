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

    // ------------------------------------------------------- Trabalho 2 (Streams)

    /**
     * Clima rechaveado pela hora cheia ({@code 2026-09-20T03:00}). E lido como
     * KTable no join da F4, no padrao do Lab2 de Streams (UserLocationTemp): a
     * medicao troca de chave para a sua hora com {@code map}, faz o join com esta
     * tabela e o Streams cria sozinho o topico de repartition. Compactado, porque
     * so interessa a ultima leitura de cada hora.
     */
    public static final String CLIMA_POR_HORA = "clima.por-hora";

    /** Intervalos [inicio, fim] por estacao, base da algebra de Allen (F5 e F6). */
    public static final String INTERVALOS = "ar.intervalos";

    private Topicos() {
    }
}
