package br.ufes.soe.ar.consumidor;

import br.ufes.soe.ar.infra.Config;
import br.ufes.soe.ar.infra.Topicos;
import br.ufes.soe.ar.modelo.CondicaoClima;
import br.ufes.soe.ar.modelo.EventoEstagnacao;
import br.ufes.soe.ar.modelo.Medicao;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Set;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * F4 - EPISODIO DE ESTAGNACAO ATMOSFERICA (evento DERIVADO).
 *
 * <p><i>"Caso o IQAr de uma estacao permaneca acima de 40 por 6 horas consecutivas,
 * sob vento fraco e sem chuva, o sistema deve inferir um episodio de estagnacao
 * atmosferica, publicar esse evento derivado no Kafka e recomendar restricao de
 * atividades ao ar livre na regiao."</i>
 *
 * <p>Este e o componente que o enunciado exige: um <b>consumidor que tambem exerce
 * o papel de produtor</b>. Ele consome eventos primitivos de DUAS fontes
 * independentes ({@code ar.medicoes} e {@code clima.condicoes}), infere conhecimento
 * novo e realimenta o Kafka com um evento composto em {@code ar.eventos-derivados}.
 *
 * <p><b>Por que isto e conhecimento inferido.</b> Nenhuma leitura isolada caracteriza
 * uma estagnacao. Um IQAr de 55 as 3h da manha e apenas um numero. O episodio so
 * existe na conjuncao de tres coisas que vivem em eventos diferentes: persistencia
 * temporal (seis horas seguidas acima do limite), condicao meteorologica que permite
 * acumulo (vento fraco) e ausencia de remocao por chuva. O evento derivado carrega
 * essa sintese, e quem o consome nao precisa refazer o raciocinio.
 *
 * <p><b>Calibracao.</b> Os limiares vieram do episodio real de 33 horas da Enseada
 * do Sua (18 a 20/09/2026). Duas janelas de seis horas satisfazem as condicoes:
 * 20/09 das 00h as 05h e das 01h as 06h, com IQAr entre 43,7 e 53,3, vento medio de
 * 4,9 a 5,1 km/h e chuva acumulada zero.
 *
 * <p>Uso: {@code java -cp target/monitoramento-ar.jar br.ufes.soe.ar.consumidor.CorrelacionadorEstagnacao}
 */
public class CorrelacionadorEstagnacao {

    private static final String GRUPO = "correlacionador-estagnacao";

    /**
     * Clima indexado por instante, preenchido pela thread de clima e lido pela thread
     * de medicoes. Um mapa navegavel permite perguntar "qual era a condicao vigente
     * nesta hora" com {@code floorEntry}, o que funciona tanto para o clima ao vivo
     * (a cada 15 min) quanto para o replay horario.
     */
    private static final NavigableMap<LocalDateTime, CondicaoClima> CLIMA =
            new ConcurrentSkipListMap<>();

    public static void main(String[] args) {
        int janelaHoras = Config.inteiro("f4.janela.horas");
        double iqarMinimo = Config.decimal("f4.iqar.minimo");
        double ventoMaximo = Config.decimal("f4.vento.maximo");
        double chuvaMaxima = Config.decimal("f4.chuva.maxima");

        System.out.printf("%s | F4: IQAr > %.0f por %dh, vento < %.1f km/h, chuva <= %.1f mm%n",
                GRUPO, iqarMinimo, janelaHoras, ventoMaximo, chuvaMaxima);

        new Thread(CorrelacionadorEstagnacao::consumirClima, "clima").start();

        // As duas threads leem do inicio do topico em paralelo. Sem esta espera, as
        // primeiras medicoes poderiam ser avaliadas com o mapa de clima ainda vazio
        // e o episodio passaria despercebido justamente no comeco do replay.
        esperarClima(Duration.ofSeconds(20));

        HistoricoEstacoes historico = new HistoricoEstacoes(janelaHoras);
        // Uma estacao so gera UM evento por episodio. Sem isto, cada hora nova
        // dentro do mesmo episodio publicaria um derivado praticamente igual.
        Set<Integer> emEpisodio = new HashSet<>();

        try (KafkaConsumer<String, Medicao> consumidor =
                     Kafka.consumidor(GRUPO, Medicao.class, Topicos.MEDICOES);
             KafkaProducer<String, Object> produtor = Kafka.produtor()) {

            while (true) {
                ConsumerRecords<String, Medicao> registros =
                        consumidor.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, Medicao> r : registros) {
                    Medicao m = r.value();
                    if (m == null || !m.operacional() || !historico.registrar(m)) {
                        continue;
                    }

                    // Saiu do limite: o episodio terminou, a estacao volta a poder
                    // gerar um novo evento no futuro.
                    if (m.iqar() <= iqarMinimo) {
                        if (emEpisodio.remove(m.idEstacao())) {
                            System.out.printf("  .. %s saiu do episodio (IQAr %.1f)%n",
                                    m.localizacao(), m.iqar());
                        }
                        continue;
                    }
                    if (emEpisodio.contains(m.idEstacao())) {
                        continue;   // ja anunciado
                    }

                    List<Medicao> janela = historico.janela(m.idEstacao(), janelaHoras - 1);
                    if (janela.size() < janelaHoras) {
                        continue;   // ainda nao ha seis horas de historico
                    }
                    if (janela.stream().anyMatch(x -> x.iqar() <= iqarMinimo)) {
                        continue;   // a persistencia foi interrompida
                    }

                    // --- correlacao com a segunda fonte de eventos ---
                    List<CondicaoClima> meteoro = climaDaJanela(janela);
                    if (meteoro.size() < janela.size()) {
                        continue;   // sem clima suficiente para afirmar o episodio
                    }
                    double ventoMedio = meteoro.stream()
                            .mapToDouble(CondicaoClima::ventoVelocidade).average().orElse(99);
                    double chuvaTotal = meteoro.stream()
                            .mapToDouble(CondicaoClima::precipitacao).sum();

                    if (ventoMedio >= ventoMaximo || chuvaTotal > chuvaMaxima) {
                        continue;
                    }

                    // --- conhecimento inferido: publica o evento derivado ---
                    EventoEstagnacao evento = new EventoEstagnacao(
                            EventoEstagnacao.TIPO,
                            m.idEstacao(),
                            m.localizacao(),
                            m.municipio(),
                            janela.get(0).dataHoraMedicao(),
                            m.dataHoraMedicao(),
                            janela.size(),
                            janela.stream().mapToDouble(Medicao::iqar).min().orElse(0),
                            janela.stream().mapToDouble(Medicao::iqar).max().orElse(0),
                            m.poluenteCritico(),
                            ventoMedio,
                            direcaoPredominante(meteoro),
                            chuvaTotal,
                            Instant.now());

                    produtor.send(new ProducerRecord<>(
                            Topicos.DERIVADOS, String.valueOf(m.idEstacao()), evento));
                    produtor.flush();
                    emEpisodio.add(m.idEstacao());

                    System.out.printf("  >> DERIVADO %s | %s%n",
                            EventoEstagnacao.TIPO, evento.descricaoCurta());
                }
            }
        }
    }

    /** Segura o inicio ate o mapa de clima ter conteudo, ou ate estourar o prazo. */
    private static void esperarClima(Duration prazo) {
        long limite = System.currentTimeMillis() + prazo.toMillis();
        while (CLIMA.isEmpty() && System.currentTimeMillis() < limite) {
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
        if (CLIMA.isEmpty()) {
            System.out.println("  (aviso: nenhum evento de clima recebido; F4 nao tera "
                    + "como correlacionar ate que o produtor-clima publique)");
        } else {
            System.out.printf("  (clima carregado: %d leituras, de %s a %s)%n",
                    CLIMA.size(), CLIMA.firstKey(), CLIMA.lastKey());
        }
    }

    /** Condicao meteorologica vigente em cada hora da janela. */
    private static List<CondicaoClima> climaDaJanela(List<Medicao> janela) {
        List<CondicaoClima> encontradas = new ArrayList<>();
        for (Medicao m : janela) {
            Map.Entry<LocalDateTime, CondicaoClima> e = CLIMA.floorEntry(m.dataHoraMedicao());
            if (e != null) {
                encontradas.add(e.getValue());
            }
        }
        return encontradas;
    }

    private static String direcaoPredominante(List<CondicaoClima> lista) {
        return lista.stream()
                .collect(java.util.stream.Collectors.groupingBy(
                        CondicaoClima::direcaoCardeal, java.util.stream.Collectors.counting()))
                .entrySet().stream()
                .max(Map.Entry.comparingByValue())
                .map(Map.Entry::getKey)
                .orElse("--");
    }

    /** Thread dedicada: mantem o mapa de clima sempre alimentado. */
    private static void consumirClima() {
        try (KafkaConsumer<String, CondicaoClima> c =
                     Kafka.consumidor(GRUPO + "-clima", CondicaoClima.class, Topicos.CLIMA)) {
            while (true) {
                for (ConsumerRecord<String, CondicaoClima> r : c.poll(Duration.ofMillis(1000))) {
                    if (r.value() != null && r.value().dataHora() != null) {
                        CLIMA.put(r.value().dataHora(), r.value());
                    }
                }
            }
        }
    }
}
