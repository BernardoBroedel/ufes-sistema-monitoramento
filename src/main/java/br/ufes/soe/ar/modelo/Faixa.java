package br.ufes.soe.ar.modelo;

/**
 * Faixas do Indice de Qualidade do Ar (Lei 14.850/2024, Resolucao CONAMA 506/2024).
 *
 * <p>A API do IEMA ja devolve a faixa classificada no campo {@code Faixa}. Este enum
 * existe apenas para poder ORDENAR as faixas e decidir se houve piora ou melhora —
 * nao para reclassificar o indice por conta propria.
 */
public enum Faixa {

    DESCONHECIDA(0, "Desconhecida"),
    BOA(1, "Boa"),
    MODERADA(2, "Moderada"),
    RUIM(3, "Ruim"),
    MUITO_RUIM(4, "Muito Ruim"),
    PESSIMA(5, "Pessima");

    /** Limite superior da faixa "Boa" na escala oficial. */
    public static final double LIMITE_FAIXA_BOA = 40.0;

    private final int ordem;
    private final String rotulo;

    Faixa(int ordem, String rotulo) {
        this.ordem = ordem;
        this.rotulo = rotulo;
    }

    public int ordem() {
        return ordem;
    }

    public String rotulo() {
        return rotulo;
    }

    /** Converte o texto devolvido pela API, tolerando acentos, caixa e nulo. */
    public static Faixa de(String texto) {
        if (texto == null || texto.isBlank()) {
            return DESCONHECIDA;
        }
        String n = texto.trim().toLowerCase()
                .replace('á', 'a').replace('ã', 'a').replace('â', 'a')
                .replace('é', 'e').replace('ê', 'e')
                .replace('í', 'i')
                .replace('ó', 'o').replace('ô', 'o').replace('õ', 'o')
                .replace('ú', 'u')
                .replace('ç', 'c');
        return switch (n) {
            case "boa" -> BOA;
            case "moderada" -> MODERADA;
            case "ruim" -> RUIM;
            case "muito ruim" -> MUITO_RUIM;
            case "pessima" -> PESSIMA;
            default -> DESCONHECIDA;
        };
    }

    /** true se esta faixa representa qualidade pior que a outra. */
    public boolean piorQue(Faixa outra) {
        return outra != null && this.ordem > outra.ordem;
    }

    /** true a partir de "Ruim" — a partir daqui o alerta vira severidade ALTA. */
    public boolean exigeAtencaoAlta() {
        return ordem >= RUIM.ordem;
    }
}
