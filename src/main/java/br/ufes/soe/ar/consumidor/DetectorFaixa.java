package br.ufes.soe.ar.consumidor;

import br.ufes.soe.ar.infra.Topicos;
import br.ufes.soe.ar.modelo.Alerta;
import br.ufes.soe.ar.modelo.Faixa;
import br.ufes.soe.ar.modelo.Medicao;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;

import java.time.Instant;
import java.util.Map;
import java.util.Optional;

/**
 * F2 - ULTRAPASSAGEM DA FAIXA DE QUALIDADE (situacao sobre evento primitivo).
 *
 * <p><i>"Caso o IQAr de uma estacao ultrapasse 40 e mude de faixa, o sistema deve
 * emitir alerta a populacao da regiao com a nova faixa e a recomendacao de saude
 * correspondente."</i>
 *
 * <p>A classificacao vem do campo {@code Faixa} devolvido pela propria API do IEMA,
 * e nao de uma reimplementacao da escala. Assim, se o orgao revisar os limites, o
 * sistema acompanha sozinho. O enum {@link Faixa} serve apenas para ORDENAR as
 * faixas e distinguir piora de melhora.
 */
public class DetectorFaixa {

    private static final String GRUPO = "detector-faixa";

    /** Recomendacao de saude por faixa, exibida no alerta e no painel. */
    private static final Map<Faixa, String> RECOMENDACAO = Map.of(
            Faixa.BOA, "Qualidade do ar adequada",
            Faixa.MODERADA, "Grupos sensiveis devem evitar esforco fisico prolongado ao ar livre",
            Faixa.RUIM, "Toda a populacao deve reduzir esforco fisico ao ar livre",
            Faixa.MUITO_RUIM, "Evite atividades ao ar livre; grupos sensiveis devem permanecer em ambientes fechados",
            Faixa.PESSIMA, "Emergencia: toda a populacao deve permanecer em ambientes fechados"
    );

    public static void main(String[] args) {
        // O estado anterior vem do proprio historico, e nao de um mapa a parte:
        // assim, quando o replay reinicia a serie de uma estacao, F2 reinicia junto.
        HistoricoEstacoes historico = new HistoricoEstacoes(2);

        System.out.printf("%s | F2: mudanca de faixa (limite da Boa = %.0f) | topico=%s%n",
                GRUPO, Faixa.LIMITE_FAIXA_BOA, Topicos.MEDICOES);

        try (KafkaConsumer<String, Medicao> consumidor =
                     Kafka.consumidor(GRUPO, Medicao.class, Topicos.MEDICOES);
             KafkaProducer<String, Alerta> produtor = Kafka.produtor()) {

            Kafka.consumir(consumidor, m ->
                    avaliar(m, historico)
                            .ifPresent(alerta -> Kafka.publicarAlerta(produtor, alerta)));
        }
    }

    /** Aplica a regra de F2 a uma leitura; devolve o alerta, se a faixa piorou. */
    private static Optional<Alerta> avaliar(Medicao m, HistoricoEstacoes historico) {
        if (!m.operacional() || !historico.registrar(m)) {
            return Optional.empty();
        }
        Faixa nova = m.faixaClassificada();

        // Sem leitura anterior (primeira da estacao), o Optional ja vem vazio:
        // ela so estabelece a linha de base.
        Optional<Faixa> mudouDe = historico.anterior(m.idEstacao())
                .map(Medicao::faixaClassificada)
                .filter(antiga -> nova != Faixa.DESCONHECIDA && nova != antiga);

        // Melhora nao gera alerta, so aparece no log.
        mudouDe.filter(antiga -> !nova.piorQue(antiga))
                .ifPresent(antiga -> System.out.printf("  .. %s melhorou: %s -> %s%n",
                        m.localizacao(), antiga.rotulo(), nova.rotulo()));

        return mudouDe
                .filter(nova::piorQue)
                .map(antiga -> new Alerta(
                        Alerta.Tipo.F2_MUDANCA_FAIXA,
                        nova.exigeAtencaoAlta()
                                ? Alerta.Severidade.ALTA : Alerta.Severidade.MEDIA,
                        m.idEstacao(),
                        m.localizacao(),
                        m.municipio(),
                        m.poluenteCritico(),
                        m.iqar(),
                        m.faixa(),
                        "Qualidade do ar passou de \"%s\" para \"%s\" (IQAr %.1f). %s"
                                .formatted(antiga.rotulo(), nova.rotulo(), m.iqar(),
                                        RECOMENDACAO.getOrDefault(nova, "")),
                        Instant.now()));
    }
}
