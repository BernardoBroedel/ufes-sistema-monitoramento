package br.ufes.soe.ar;

import br.ufes.soe.ar.modelo.CondicaoClima;
import br.ufes.soe.ar.modelo.Medicao;

import java.time.Instant;
import java.time.LocalDateTime;

/** Eventos de exemplo para os testes, no formato que os produtores publicam. */
public final class Amostras {

    /** Data sentinela que a API do IEMA devolve para estacao offline. */
    public static final LocalDateTime SENTINELA = LocalDateTime.of(1, 1, 1, 0, 0);

    private Amostras() {
    }

    public static Medicao medicao(int idEstacao, LocalDateTime quando, double iqar) {
        return new Medicao(idEstacao, "RGV" + idEstacao, "Enseada do Sua", "Vitoria",
                -20.31, -40.29, "MP10", 30.0, iqar, iqar > 40 ? "Moderada" : "Boa",
                quando, Instant.now());
    }

    /** Leitura de estacao fora do ar: poluente nulo, IQAr zero, data sentinela. */
    public static Medicao offline(int idEstacao) {
        return new Medicao(idEstacao, "RGV" + idEstacao, "Carapina", "Serra",
                -20.23, -40.27, null, null, 0.0, null, SENTINELA, Instant.now());
    }

    public static CondicaoClima clima(LocalDateTime quando, double vento, double chuva) {
        return new CondicaoClima(quando, 24.0, chuva, vento, 45, Instant.now());
    }
}
