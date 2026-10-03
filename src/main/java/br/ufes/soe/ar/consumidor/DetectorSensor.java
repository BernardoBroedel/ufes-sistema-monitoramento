package br.ufes.soe.ar.consumidor;

import br.ufes.soe.ar.infra.Config;
import br.ufes.soe.ar.infra.Topicos;
import br.ufes.soe.ar.modelo.Alerta;
import br.ufes.soe.ar.modelo.Medicao;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Set;

/**
 * F3 - ESTACAO FORA DE OPERACAO (situacao sobre evento primitivo).
 *
 * <p><i>"Caso uma estacao deixe de reportar medicoes validas por mais de 2 horas, o
 * sistema deve emitir alerta de manutencao e marcar a estacao como indisponivel,
 * para que o silencio nao seja interpretado como ausencia de poluicao."</i>
 *
 * <p>Esta situacao nao e hipotetica: a estacao RGV2 (Carapina) esteve fora do ar
 * durante todo o periodo analisado em 20/09/2026, devolvendo {@code "Poluente": null},
 * {@code "Iqa": 0.0} e a data sentinela {@code 0001-01-01T00:00:00}.
 *
 * <p>Observe a inversao em relacao as outras funcionalidades: aqui o que interessa
 * e a AUSENCIA de informacao. Num sistema de monitoramento, tratar silencio como
 * "tudo bem" e o erro mais perigoso que existe.
 *
 * <p><b>Tempo do stream, nao do relogio.</b> O atraso e medido contra a medicao mais
 * recente ja vista no topico, e nao contra {@code LocalDateTime.now()}. Comparar com
 * o relogio parece natural, mas quebra no replay: leituras historicas de dois dias
 * atras apareceriam como "47 horas atrasadas" e gerariam alarme falso em TODAS as
 * estacoes. Como o sistema raciocina sobre eventos, a referencia de tempo tem que
 * vir dos proprios eventos.
 */
public class DetectorSensor {

    private static final String GRUPO = "detector-sensor";

    public static void main(String[] args) {
        int atrasoMaximo = Config.inteiro("f3.atraso.maximo.horas");
        int saltoReplay = Config.inteiro("f3.salto.replay.horas");
        // Evita repetir o mesmo alerta a cada polling enquanto a estacao segue fora.
        Set<Integer> jaAlertadas = new HashSet<>();
        // "Agora" segundo o stream: a medicao mais recente ja observada no topico.
        LocalDateTime tempoDoStream = null;

        System.out.printf("%s | F3: sem medicao valida ha mais de %dh | topico=%s%n",
                GRUPO, atrasoMaximo, Topicos.MEDICOES);

        try (KafkaConsumer<String, Medicao> consumidor =
                     Kafka.consumidor(GRUPO, Medicao.class, Topicos.MEDICOES);
             KafkaProducer<String, Alerta> produtor = Kafka.produtor()) {

            while (true) {
                ConsumerRecords<String, Medicao> registros =
                        consumidor.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, Medicao> r : registros) {
                    Medicao m = r.value();
                    if (m == null) {
                        continue;
                    }

                    // Avanca o relogio do stream antes de julgar qualquer estacao.
                    if (m.operacional()) {
                        if (tempoDoStream == null
                                || m.dataHoraMedicao().isAfter(tempoDoStream)) {
                            tempoDoStream = m.dataHoraMedicao();

                        } else if (Duration.between(m.dataHoraMedicao(), tempoDoStream)
                                .toHours() >= saltoReplay) {
                            // Salto grande para tras: o produtor-replay comecou a
                            // reproduzir o historico. Se o relogio do stream ficasse
                            // no futuro, TODAS as estacoes do replay pareceriam
                            // atrasadas. Recua o relogio e limpa os alertas antigos.
                            System.out.printf("  ~~ replay detectado: relogio do stream "
                                    + "recua de %s para %s%n", tempoDoStream, m.dataHoraMedicao());
                            tempoDoStream = m.dataHoraMedicao();
                            jaAlertadas.clear();
                        }
                    }
                    if (tempoDoStream == null) {
                        continue;   // ainda nao ha referencia temporal
                    }

                    long horasAtraso = m.atraso(tempoDoStream).toHours();
                    boolean atrasada = m.operacional() && horasAtraso > atrasoMaximo;
                    boolean fora = !m.operacional() || atrasada;

                    if (!fora) {
                        // Voltou a operar: limpa o estado e avisa a recuperacao.
                        if (jaAlertadas.remove(m.idEstacao())) {
                            System.out.printf("  .. %s voltou a reportar (IQAr %.1f)%n",
                                    m.localizacao(), m.iqar());
                        }
                        continue;
                    }

                    if (!jaAlertadas.add(m.idEstacao())) {
                        continue;   // ja alertada, segue fora do ar
                    }

                    String motivo = !m.operacional()
                            ? "estacao nao reporta poluente nem data valida"
                            : "ultima medicao ha %d horas (referencia do stream: %s)"
                                    .formatted(horasAtraso, tempoDoStream);

                    Kafka.publicarAlerta(produtor, new Alerta(
                            Alerta.Tipo.F3_ESTACAO_OFFLINE,
                            Alerta.Severidade.MEDIA,
                            m.idEstacao(),
                            m.localizacao(),
                            m.municipio(),
                            m.poluenteCritico(),
                            m.iqar(),
                            m.faixa(),
                            "Estacao indisponivel para monitoramento: " + motivo
                                    + ". Acionar manutencao.",
                            Instant.now()));
                }
            }
        }
    }
}
