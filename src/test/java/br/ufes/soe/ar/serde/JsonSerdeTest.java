package br.ufes.soe.ar.serde;

import br.ufes.soe.ar.Amostras;
import br.ufes.soe.ar.modelo.Medicao;
import org.junit.jupiter.api.Test;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class JsonSerdeTest {

    private final JsonSerde<Medicao> serde = JsonSerde.de(Medicao.class);

    @Test
    void idaEVoltaPreservaOEvento() {
        Medicao m = Amostras.medicao(4, LocalDateTime.of(2026, 9, 20, 3, 0), 53.3);

        byte[] bytes = serde.serializer().serialize("ar.medicoes", m);
        assertEquals(m, serde.deserializer().deserialize("ar.medicoes", bytes));
    }

    @Test
    void jsonInvalidoViraNuloEmVezDeDerrubarAAplicacao() {
        byte[] lixo = "{nao e json".getBytes(StandardCharsets.UTF_8);
        assertNull(serde.deserializer().deserialize("ar.medicoes", lixo));
    }
}
