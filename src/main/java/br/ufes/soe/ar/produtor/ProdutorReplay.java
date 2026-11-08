package br.ufes.soe.ar.produtor;

import br.ufes.soe.ar.infra.ClienteIema;
import br.ufes.soe.ar.infra.ClienteOpenMeteo;
import br.ufes.soe.ar.infra.Config;
import br.ufes.soe.ar.infra.Estacoes;
import br.ufes.soe.ar.infra.Topicos;
import br.ufes.soe.ar.modelo.CondicaoClima;
import br.ufes.soe.ar.modelo.Medicao;
import br.ufes.soe.ar.serde.JsonSerializer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * PRODUTOR DE REPLAY. Le as ~48h de historico das estacoes e republica em
 * {@code ar.medicoes} em escala acelerada.
 *
 * <p>Existe por uma razao pratica: as oito estacoes produzem uma leitura por hora,
 * e na maior parte do tempo todas estao na faixa "Boa". Ao vivo, uma apresentacao
 * de vinte minutos nao veria alerta nenhum. Com o replay, dois dias de dados reais
 * passam em menos de um minuto e as funcionalidades disparam na frente da turma.
 *
 * <p>Nao e dado sintetico: e o historico verdadeiro da rede do IEMA, apenas com a
 * linha do tempo comprimida. O episodio de 33 horas da Enseada do Sua (estacao 4)
 * aciona F1, F2 e F4 de uma vez so.
 *
 * <p>Uso:
 * <pre>
 *   java -cp target/monitoramento-ar.jar br.ufes.soe.ar.produtor.ProdutorReplay
 *   java -cp target/monitoramento-ar.jar br.ufes.soe.ar.produtor.ProdutorReplay 4
 *   java -cp target/monitoramento-ar.jar br.ufes.soe.ar.produtor.ProdutorReplay 4 200
 * </pre>
 * O primeiro argumento e a lista de estacoes (separada por virgula, ou "todas");
 * o segundo e quantos milissegundos representam uma hora real.
 */
public class ProdutorReplay {

    public static void main(String[] args) throws Exception {
        List<Integer> estacoes = estacoesDe(args.length > 0 ? args[0] : "todas");
        long msPorHora = args.length > 1 ? Long.parseLong(args[1]) : 1000L;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, Config.bootstrap());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, JsonSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");

        ClienteIema iema = new ClienteIema();

        System.out.println("produtor-replay | estacoes=" + estacoes
                + " | 1 hora real = " + msPorHora + " ms");

        // Ordena por tempo: as estacoes precisam avancar juntas, hora a hora,
        // senao a correlacao de F4 com o clima nao faz sentido.
        List<Medicao> linhaDoTempo = estacoes.stream()
                .flatMap(id -> historicoDe(iema, id).stream())
                .sorted(Comparator.comparing(Medicao::dataHoraMedicao))
                .toList();

        // O clima entra junto: sem ele, a janela de F4 nao teria condicao
        // meteorologica para correlacionar e o evento derivado nunca sairia.
        List<CondicaoClima> climaHistorico = climaHistorico();

        if (linhaDoTempo.isEmpty()) {
            System.err.println("nenhum historico obtido, nada a reproduzir");
            return;
        }

        System.out.printf("%nreproduzindo %d eventos de %s ate %s%n%n",
                linhaDoTempo.size(),
                linhaDoTempo.get(0).dataHoraMedicao(),
                linhaDoTempo.get(linhaDoTempo.size() - 1).dataHoraMedicao());

        // Indexa o clima por hora, para publicar a condicao correspondente
        // imediatamente antes das medicoes daquela hora.
        Map<LocalDateTime, CondicaoClima> climaPorHora = climaHistorico.stream()
                .collect(Collectors.toMap(
                        c -> horaCheia(c.dataHora()),
                        Function.identity(),
                        (anterior, posterior) -> posterior));

        // Medicoes agrupadas por hora cheia; o TreeMap mantem as horas em ordem e
        // o groupingBy preserva, dentro de cada hora, a ordem da linha do tempo.
        Map<LocalDateTime, List<Medicao>> porHora = linhaDoTempo.stream()
                .collect(Collectors.groupingBy(
                        m -> horaCheia(m.dataHoraMedicao()),
                        TreeMap::new,
                        Collectors.toList()));

        try (KafkaProducer<String, Object> produtor = new KafkaProducer<>(props)) {
            boolean primeiraHora = true;

            // Laco classico de proposito: Thread.sleep lanca InterruptedException
            // (checada), que um lambda de forEach nao pode propagar.
            for (Map.Entry<LocalDateTime, List<Medicao>> hora : porHora.entrySet()) {
                if (!primeiraHora) {
                    Thread.sleep(msPorHora);
                }
                primeiraHora = false;

                Optional.ofNullable(climaPorHora.get(hora.getKey())).ifPresent(c -> {
                    produtor.send(new ProducerRecord<>(Topicos.CLIMA, "grande-vitoria", c));
                    System.out.printf("  %s  %-20s %s%n",
                            hora.getKey(), "[clima]", c.descricaoCurta());
                });

                hora.getValue().forEach(m -> {
                    produtor.send(new ProducerRecord<>(
                            Topicos.MEDICOES, String.valueOf(m.idEstacao()), m));
                    System.out.printf("  %s  %-20s IQAr %6.1f  [%s]%n",
                            m.dataHoraMedicao(), m.localizacao(), m.iqar(), m.faixa());
                });
            }
            produtor.flush();
        }
        System.out.println("\nreplay concluido.");
    }

    /**
     * Historico de uma estacao, ou lista vazia se a API falhar. A excecao e tratada
     * aqui, e nao no stream, porque um lambda nao pode lancar excecao checada.
     */
    private static List<Medicao> historicoDe(ClienteIema iema, int id) {
        try {
            List<Medicao> h = iema.lerHistorico(id);
            System.out.printf("  %-20s %3d leituras historicas%n",
                    Estacoes.localizacao(id), h.size());
            return h;
        } catch (Exception e) {
            System.err.printf("  %-20s falhou: %s%n", Estacoes.localizacao(id), e.getMessage());
            return List.of();
        }
    }

    private static List<CondicaoClima> climaHistorico() {
        try {
            List<CondicaoClima> clima = new ClienteOpenMeteo().lerHistorico(3);
            System.out.printf("  %-20s %3d leituras horarias%n", "clima", clima.size());
            return clima;
        } catch (Exception e) {
            System.err.println("  clima historico falhou: " + e.getMessage());
            return List.of();
        }
    }

    private static LocalDateTime horaCheia(LocalDateTime t) {
        return t.truncatedTo(ChronoUnit.HOURS);
    }

    private static List<Integer> estacoesDe(String arg) {
        if (arg.equalsIgnoreCase("todas")) {
            return Estacoes.GRANDE_VITORIA.stream().sorted().toList();
        }
        return Arrays.stream(arg.split(","))
                .map(String::trim)
                .map(Integer::parseInt)
                .toList();
    }
}
