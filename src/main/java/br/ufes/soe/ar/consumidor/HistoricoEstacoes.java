package br.ufes.soe.ar.consumidor;

import br.ufes.soe.ar.modelo.Medicao;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * StateStore em memoria, no mesmo padrao do {@code StudentStateStore} do Lab2:
 * guarda o estado anterior de cada entidade para poder compara-lo com o novo.
 *
 * <p>Aqui a entidade e a estacao, e o estado e a serie recente de leituras. Sem
 * isso nenhuma das funcionalidades funcionaria: F1 precisa do IQAr de tres horas
 * atras, F2 precisa da faixa anterior e F4 precisa da janela inteira de seis horas.
 *
 * <p><b>Descarte de repetidos.</b> O metodo {@link #registrar} devolve false quando
 * a leitura tem o mesmo carimbo de tempo da ultima ja vista para aquela estacao.
 * Isso protege os detectores de duas coisas: da entrega "pelo menos uma vez" do
 * Kafka e de eventuais republicacoes do produtor.
 */
public class HistoricoEstacoes {

    private final int horasGuardadas;
    private final Map<Integer, Deque<Medicao>> porEstacao = new HashMap<>();
    private final Map<Integer, LocalDateTime> ultimoCarimbo = new HashMap<>();

    public HistoricoEstacoes(int horasGuardadas) {
        this.horasGuardadas = horasGuardadas;
    }

    /**
     * Registra a leitura. Devolve false se for repetida — nesse caso o detector
     * deve ignorar o evento por completo.
     */
    public boolean registrar(Medicao m) {
        if (m == null || m.dataHoraMedicao() == null) {
            return false;
        }
        LocalDateTime anterior = ultimoCarimbo.get(m.idEstacao());

        if (anterior != null && !m.dataHoraMedicao().isAfter(anterior)) {
            // A leitura nao avanca no tempo. Ha dois casos bem diferentes aqui.
            long horasAtras = Duration.between(m.dataHoraMedicao(), anterior).toHours();

            if (horasAtras <= horasGuardadas) {
                // Repetida ou levemente fora de ordem: descarta. E o caso comum,
                // causado pela entrega "pelo menos uma vez" do Kafka.
                return false;
            }
            // Salto grande para tras: e o produtor-replay reproduzindo o historico.
            // Manter o estado antigo faria o detector descartar o replay inteiro,
            // entao a serie daquela estacao recomeca do zero.
            porEstacao.remove(m.idEstacao());
            System.out.printf("  ~~ %s: sequencia reiniciada (%s vem %dh antes de %s)%n",
                    m.localizacao(), m.dataHoraMedicao(), horasAtras, anterior);
        }
        ultimoCarimbo.put(m.idEstacao(), m.dataHoraMedicao());

        Deque<Medicao> fila = porEstacao.computeIfAbsent(m.idEstacao(), k -> new ArrayDeque<>());
        fila.addLast(m);
        // guarda um ponto por hora, com folga
        while (fila.size() > horasGuardadas + 2) {
            fila.removeFirst();
        }
        return true;
    }

    /** Leitura imediatamente anterior a atual, ou null se ainda nao houver. */
    public Medicao anterior(int idEstacao) {
        Deque<Medicao> fila = porEstacao.get(idEstacao);
        if (fila == null || fila.size() < 2) {
            return null;
        }
        List<Medicao> lista = List.copyOf(fila);
        return lista.get(lista.size() - 2);
    }

    /**
     * Leitura mais antiga dentro da janela de {@code horas} a contar da leitura
     * mais recente. Devolve null se ainda nao ha historico suficiente.
     */
    public Medicao haHoras(int idEstacao, int horas) {
        Deque<Medicao> fila = porEstacao.get(idEstacao);
        if (fila == null || fila.isEmpty()) {
            return null;
        }
        LocalDateTime alvo = fila.peekLast().dataHoraMedicao().minusHours(horas);
        Medicao candidata = null;
        for (Medicao m : fila) {
            if (!m.dataHoraMedicao().isAfter(alvo)) {
                candidata = m;      // a mais recente que ainda esta fora da janela
            }
        }
        return candidata;
    }

    /** Todas as leituras dentro das ultimas {@code horas}, da mais antiga para a mais nova. */
    public List<Medicao> janela(int idEstacao, int horas) {
        Deque<Medicao> fila = porEstacao.get(idEstacao);
        if (fila == null || fila.isEmpty()) {
            return List.of();
        }
        LocalDateTime corte = fila.peekLast().dataHoraMedicao().minusHours(horas).minusMinutes(1);
        return fila.stream()
                .filter(m -> m.dataHoraMedicao().isAfter(corte))
                .toList();
    }

    public int estacoesConhecidas() {
        return porEstacao.size();
    }
}
