package br.ufes.soe.ar.consumidor;

import br.ufes.soe.ar.modelo.Alerta;
import br.ufes.soe.ar.modelo.CondicaoClima;
import br.ufes.soe.ar.modelo.Medicao;

import java.util.ArrayDeque;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Estado que o painel exibe: a ultima leitura de cada estacao, os alertas mais
 * recentes e os contadores por topico.
 *
 * <p>E alimentado por duas threads de consumo (medicoes e alertas) e lido pela
 * thread do servidor HTTP, entao as estruturas precisam ser thread-safe. O
 * {@link #alertas} usa bloco sincronizado porque um Deque comum nao e.
 *
 * <p>Repare que o painel nunca consulta a API do IEMA: tudo o que ele mostra
 * chegou pelo Kafka. E o que torna visivel, na apresentacao, que o Kafka e o
 * middleware do sistema, e nao um detalhe de implementacao.
 */
public class EstadoPainel {

    /**
     * Quantos alertas manter na tela, POR TIPO. Um teto unico para todos fazia uma
     * rajada de F1 (o reprocessamento do topico gera dezenas) empurrar para fora o
     * unico F4 — justamente o alerta mais importante.
     */
    private static final int MAX_POR_TIPO = 25;

    private final Map<Integer, Medicao> estacoes = new ConcurrentHashMap<>();
    private final Deque<Alerta> alertas = new ArrayDeque<>();
    private final AtomicLong totalMedicoes = new AtomicLong();
    private final AtomicLong totalAlertas = new AtomicLong();
    private final AtomicLong totalDerivados = new AtomicLong();
    private volatile CondicaoClima clima;

    public void registrarMedicao(Medicao m) {
        totalMedicoes.incrementAndGet();
        estacoes.merge(m.idEstacao(), m, (antiga, nova) ->
                nova.dataHoraMedicao() == null ? antiga
                        : antiga.dataHoraMedicao() == null ? nova
                        : nova.dataHoraMedicao().isAfter(antiga.dataHoraMedicao()) ? nova : antiga);
    }

    public void registrarClima(CondicaoClima c) {
        this.clima = c;
    }

    public void registrarAlerta(Alerta a) {
        totalAlertas.incrementAndGet();
        if (a.tipo() == Alerta.Tipo.F4_ESTAGNACAO) {
            totalDerivados.incrementAndGet();
        }
        synchronized (alertas) {
            alertas.addFirst(a);
            descartarExcedente(a.tipo());
        }
    }

    /** Remove o alerta mais antigo do tipo, se ele passou do limite. Chamar com o lock. */
    private void descartarExcedente(Alerta.Tipo tipo) {
        long doTipo = alertas.stream().filter(x -> x.tipo() == tipo).count();
        if (doTipo <= MAX_POR_TIPO) {
            return;
        }
        Iterator<Alerta> it = alertas.descendingIterator();
        while (it.hasNext()) {
            if (it.next().tipo() == tipo) {
                it.remove();
                return;
            }
        }
    }

    /** Fotografia do estado, no formato que o endpoint {@code /api/estado} devolve. */
    public Map<String, Object> instantaneo() {
        List<Alerta> copia;
        synchronized (alertas) {
            copia = List.copyOf(alertas);
        }
        Map<String, Object> contadores = new LinkedHashMap<>();
        contadores.put("medicoes", totalMedicoes.get());
        contadores.put("alertas", totalAlertas.get());
        contadores.put("derivados", totalDerivados.get());

        Map<String, Object> raiz = new LinkedHashMap<>();
        raiz.put("atualizadoEm", java.time.Instant.now().toString());
        raiz.put("estacoes", estacoes.values().stream()
                .sorted((a, b) -> Integer.compare(a.idEstacao(), b.idEstacao()))
                .map(EstadoPainel::comoMapa)
                .toList());
        raiz.put("clima", clima);
        raiz.put("alertas", copia);
        raiz.put("contadores", contadores);
        return raiz;
    }

    /** Achata a Medicao e acrescenta o campo derivado {@code operacional}. */
    private static Map<String, Object> comoMapa(Medicao m) {
        Map<String, Object> mapa = new LinkedHashMap<>();
        mapa.put("idEstacao", m.idEstacao());
        mapa.put("localizacao", m.localizacao());
        mapa.put("municipio", m.municipio());
        mapa.put("poluenteCritico", m.poluenteCritico());
        mapa.put("iqar", m.iqar());
        mapa.put("faixa", m.faixa());
        mapa.put("dataHoraMedicao",
                m.dataHoraMedicao() == null ? null : m.dataHoraMedicao().toString());
        mapa.put("operacional", m.operacional());
        return mapa;
    }
}
