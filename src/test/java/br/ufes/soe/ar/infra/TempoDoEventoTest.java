package br.ufes.soe.ar.infra;

import br.ufes.soe.ar.Amostras;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class TempoDoEventoTest {

    @Test
    void converteHorarioDeSaoPauloParaUtc() {
        // Sem horario de verao desde 2019: Sao Paulo e sempre UTC-3.
        LocalDateTime local = LocalDateTime.of(2026, 9, 20, 3, 0);
        long esperado = Instant.parse("2026-09-20T06:00:00Z").toEpochMilli();

        assertEquals(esperado, TempoDoEvento.epochMillis(local));
        assertEquals(local, TempoDoEvento.deEpochMillis(esperado));
    }

    @Test
    void dataAusenteOuSentinelaNaoTemTempo() {
        assertNull(TempoDoEvento.epochMillis(null));
        assertNull(TempoDoEvento.epochMillis(Amostras.SENTINELA));
        assertNull(TempoDoEvento.paraRegistro(Amostras.SENTINELA));
    }

    @Test
    void produtorNaoGravaTimestampNoFuturo() {
        assertNull(TempoDoEvento.paraRegistro(LocalDateTime.now(TempoDoEvento.zona()).plusDays(1)));
    }

    @Test
    void produtorGravaTimestampHistorico() {
        LocalDateTime anteontem = LocalDateTime.now(TempoDoEvento.zona()).minusDays(2);
        assertEquals(TempoDoEvento.epochMillis(anteontem), TempoDoEvento.paraRegistro(anteontem));
    }
}
