package br.ufes.soe.ar.infra;

import java.util.Map;
import java.util.Set;

/**
 * As oito estacoes da Regiao da Grande Vitoria (prefixo RGV na rede do IEMA).
 *
 * <p>A API expoe 14 estacoes: estas oito mais seis da rede SUL (Anchieta e
 * Guarapari), que ficam fora do escopo do projeto e sao filtradas na coleta.
 *
 * <p>A API devolve o bairro em {@code Localizacao} mas nao o municipio, entao o
 * mapeamento abaixo completa o dado para os alertas e para o painel.
 */
public final class Estacoes {

    /** Ids das estacoes da Grande Vitoria. Nao ha RGV7 nem RGV10 na resposta da API. */
    public static final Set<Integer> GRANDE_VITORIA = Set.of(1, 2, 3, 4, 5, 6, 8, 9);

    private static final Map<Integer, String> MUNICIPIO = Map.of(
            1, "Serra",         // Laranjeiras
            2, "Serra",         // Carapina
            3, "Vitória",       // Jardim Camburi
            4, "Vitória",       // Enseada do Suá
            5, "Vitória",       // Vitória - Centro
            6, "Vila Velha",    // Vila Velha - IBES
            8, "Cariacica",     // Vila Capixaba
            9, "Serra"          // Cidade Continental
    );

    private static final Map<Integer, String> LOCALIZACAO = Map.of(
            1, "Laranjeiras",
            2, "Carapina",
            3, "Jardim Camburi",
            4, "Enseada do Suá",
            5, "Vitória - Centro",
            6, "Vila Velha - IBES",
            8, "Vila Capixaba",
            9, "Cidade Continental"
    );

    private Estacoes() {
    }

    /**
     * Bairro da estacao. O snapshot de {@code /api/mapa} traz esse dado, mas o
     * endpoint por estacao nao — por isso o replay depende deste mapa.
     */
    public static String localizacao(int idEstacao) {
        return LOCALIZACAO.getOrDefault(idEstacao, "estacao " + idEstacao);
    }

    public static boolean ehGrandeVitoria(int idEstacao) {
        return GRANDE_VITORIA.contains(idEstacao);
    }

    public static String municipio(int idEstacao) {
        return MUNICIPIO.getOrDefault(idEstacao, "Desconhecido");
    }
}
