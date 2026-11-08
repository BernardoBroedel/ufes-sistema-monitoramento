package br.ufes.soe.ar.consumidor;

import br.ufes.soe.ar.infra.Config;
import br.ufes.soe.ar.infra.Topicos;
import br.ufes.soe.ar.modelo.Alerta;
import br.ufes.soe.ar.modelo.Medicao;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.time.Instant;
import java.util.Optional;

/**
 * F1 - PIORA ACELERADA DA QUALIDADE DO AR (situacao sobre evento primitivo).
 *
 * <p><i>"Caso o IQAr de uma estacao suba mais de 3 pontos em 3 horas, o sistema deve
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

            Kafka.consumir(consumidor, atual ->
                    avaliar(atual, historico, janelaHoras, deltaMinimo)
                            .ifPresent(alerta -> Kafka.publicarAlerta(produtor, alerta)));
        }
    }

    /** Aplica a regra de F1 a uma leitura; devolve o alerta, se a regra disparou. */
    private static Optional<Alerta> avaliar(Medicao atual, HistoricoEstacoes historico,
                                            int janelaHoras, double deltaMinimo) {
        // estacao offline e assunto de F3; leitura repetida e descartada
        if (!atual.operacional() || !historico.registrar(atual)) {
            return Optional.empty();
        }
        // vazio enquanto o historico da estacao ainda nao cobre a janela
        return historico.haHoras(atual.idEstacao(), janelaHoras)
                .filter(referencia -> atual.iqar() - referencia.iqar() > deltaMinimo)
                .map(referencia -> new Alerta(
                        Alerta.Tipo.F1_SUBIDA_ACELERADA,
                        Alerta.Severidade.MEDIA,
                        atual.idEstacao(),
                        atual.localizacao(),
                        atual.municipio(),
                        atual.poluenteCritico(),
                        atual.iqar(),
                        atual.faixa(),
                        "IQAr subiu %.1f pontos em %dh (de %.1f para %.1f)"
                                .formatted(atual.iqar() - referencia.iqar(), janelaHoras,
                                        referencia.iqar(), atual.iqar()),
                        Instant.now()));
    }
}
