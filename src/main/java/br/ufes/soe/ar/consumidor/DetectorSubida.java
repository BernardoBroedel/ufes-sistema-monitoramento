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

/**
 * F1 - PIORA ACELERADA DA QUALIDADE DO AR (situacao sobre evento primitivo).
 *
 * <p><i>"Caso o IQAr de uma estacao suba mais de 5 pontos em 3 horas, o sistema deve
 * notificar os gestores ambientais informando a estacao, o poluente critico, o valor
 * atual e a variacao observada."</i>
 *
 * <p><b>Por que tendencia e nao nivel.</b> Cada estacao tem linha de base propria: a
 * mediana vai de 3,4 em Laranjeiras a 53,3 na Enseada do Sua. Um limiar absoluto
 * baixo dispararia sem parar numa e nunca na outra. Medir a subida torna as sete
 * estacoes comparaveis. Na calibracao sobre 48h reais, o maior salto do periodo foi
 * de 16,7 pontos em Jardim Camburi — o bairro do po preto aparece no topo justamente
 * nesta metrica, e nao na de nivel absoluto.
 */
public class DetectorSubida {

    private static final String GRUPO = "detector-subida";

    public static void main(String[] args) {
        int janelaHoras = Config.inteiro("f1.janela.horas");
        double deltaMinimo = Config.decimal("f1.delta.minimo");
        HistoricoEstacoes historico = new HistoricoEstacoes(janelaHoras);

        System.out.printf("%s | F1: subida > %.1f pontos em %dh | topico=%s%n",
                GRUPO, deltaMinimo, janelaHoras, Topicos.MEDICOES);

        try (KafkaConsumer<String, Medicao> consumidor =
                     Kafka.consumidor(GRUPO, Medicao.class, Topicos.MEDICOES);
             KafkaProducer<String, Alerta> produtor = Kafka.produtor()) {

            while (true) {
                ConsumerRecords<String, Medicao> registros =
                        consumidor.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, Medicao> r : registros) {
                    Medicao atual = r.value();
                    if (atual == null || !atual.operacional()) {
                        continue;   // estacao offline e assunto de F3
                    }
                    if (!historico.registrar(atual)) {
                        continue;   // leitura repetida
                    }

                    Medicao referencia = historico.haHoras(atual.idEstacao(), janelaHoras);
                    if (referencia == null) {
                        continue;   // historico ainda insuficiente para esta estacao
                    }

                    double delta = atual.iqar() - referencia.iqar();
                    if (delta <= deltaMinimo) {
                        continue;
                    }

                    Kafka.publicarAlerta(produtor, new Alerta(
                            Alerta.Tipo.F1_SUBIDA_ACELERADA,
                            Alerta.Severidade.MEDIA,
                            atual.idEstacao(),
                            atual.localizacao(),
                            atual.municipio(),
                            atual.poluenteCritico(),
                            atual.iqar(),
                            atual.faixa(),
                            "IQAr subiu %.1f pontos em %dh (de %.1f para %.1f)"
                                    .formatted(delta, janelaHoras,
                                            referencia.iqar(), atual.iqar()),
                            Instant.now()));
                }
            }
        }
    }
}
