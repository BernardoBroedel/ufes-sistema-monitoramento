package br.ufes.soe.ar.infra;

import br.ufes.soe.ar.modelo.Medicao;
import br.ufes.soe.ar.serde.Json;
import com.fasterxml.jackson.databind.JsonNode;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.Instant;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Cliente da API publica de qualidade do ar do Governo do ES.
 *
 * <pre>
 *   GET https://qualidadedoarapi.es.gov.br/api/mapa        snapshot das 14 estacoes
 *   GET https://qualidadedoarapi.es.gov.br/api/mapa/{id}   ~48h de serie por poluente
 * </pre>
 *
 * <p>Sem autenticacao e sem cota. Enviamos {@code User-Agent} e {@code Referer}
 * por cortesia, identificando o cliente — mas eles NAO sao obrigatorios: verificado
 * em 21/09/2026, a API responde normalmente sem header nenhum.
 *
 * <p>Ainda assim, o codigo trata resposta com corpo vazio como erro. Isso ja foi
 * observado uma vez, aparentemente em cold start do servidor, e um corpo vazio e
 * facil de confundir com "nenhuma estacao reportando" — que e exatamente o tipo de
 * silencio que este sistema nao pode interpretar como normalidade.
 *
 * <p>A leitura e feita com JsonNode, e nao com um DTO mapeado, porque a resposta
 * usa PascalCase e traz campos nulos quando a estacao esta offline — condicao que
 * precisa chegar intacta ate a funcionalidade F3.
 */
public class ClienteIema {

    private static final String BASE = "https://qualidadedoarapi.es.gov.br/api/mapa";

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(15))
            .followRedirects(HttpClient.Redirect.NORMAL)
            .build();

    private final String userAgent = Config.texto("iema.user.agent");
    private final String referer = Config.texto("iema.referer");

    /** Snapshot atual, ja filtrado para as estacoes da Grande Vitoria. */
    public List<Medicao> lerEstacoes() throws Exception {
        JsonNode raiz = buscar(BASE);
        Instant agora = Instant.now();
        List<Medicao> medicoes = new ArrayList<>();

        for (JsonNode no : raiz) {
            int id = no.path("IdEstacao").asInt();
            if (!Estacoes.ehGrandeVitoria(id)) {
                continue;
            }
            medicoes.add(new Medicao(
                    id,
                    texto(no, "Descricao"),
                    texto(no, "Localizacao"),
                    Estacoes.municipio(id),
                    no.path("Latitude").asDouble(),
                    no.path("Longitude").asDouble(),
                    texto(no, "Poluente"),
                    no.path("Valor").isNumber() ? no.path("Valor").asDouble() : null,
                    no.path("Iqa").asDouble(),
                    texto(no, "Faixa"),
                    dataHora(no.path("DataHora")),
                    agora));
        }
        return medicoes;
    }

    /**
     * Serie horaria das ultimas ~48h de uma estacao, ja reduzida a uma leitura por
     * hora.
     *
     * <p>O endpoint por estacao devolve uma serie separada para cada poluente. O
     * IQAr da estacao, porem, e o do PIOR poluente naquela hora — e assim que o
     * snapshot de {@code /api/mapa} o calcula. Este metodo reproduz essa reducao:
     * para cada hora, toma o maior valor entre os poluentes e carrega junto qual
     * foi o responsavel.
     */
    public List<Medicao> lerHistorico(int idEstacao) throws Exception {
        JsonNode poluentes = buscar(BASE + "/" + idEstacao);
        Map<LocalDateTime, Medicao> porHora = new TreeMap<>();
        Instant agora = Instant.now();

        for (JsonNode p : poluentes) {
            String nomePoluente = texto(p, "Poluente");
            JsonNode datas = p.path("DataHora");
            JsonNode valores = p.path("ValorIqa");
            JsonNode faixas = p.path("FaixaIQA");

            for (int i = 0; i < datas.size(); i++) {
                LocalDateTime hora = dataHora(datas.get(i));
                if (hora == null || i >= valores.size()) {
                    continue;
                }
                double iqar;
                try {
                    iqar = Double.parseDouble(valores.get(i).asText());
                } catch (NumberFormatException e) {
                    continue;
                }
                Medicao existente = porHora.get(hora);
                if (existente != null && existente.iqar() >= iqar) {
                    continue;   // outro poluente ja esta pior nesta hora
                }
                porHora.put(hora, new Medicao(
                        idEstacao,
                        "EMQAr - RGV" + idEstacao,
                        Estacoes.localizacao(idEstacao),
                        Estacoes.municipio(idEstacao),
                        0, 0,
                        nomePoluente,
                        iqar,
                        iqar,
                        i < faixas.size() ? faixas.get(i).asText() : null,
                        hora,
                        agora));
            }
        }
        return new ArrayList<>(porHora.values());
    }

    private JsonNode buscar(String url) throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .header("User-Agent", userAgent)
                .header("Referer", referer)
                .header("Accept", "application/json")
                .timeout(Duration.ofSeconds(40))
                .GET()
                .build();

        HttpResponse<byte[]> resp = http.send(req, HttpResponse.BodyHandlers.ofByteArray());
        if (resp.statusCode() != 200) {
            throw new IllegalStateException("IEMA respondeu HTTP " + resp.statusCode());
        }
        if (resp.body() == null || resp.body().length == 0) {
            throw new IllegalStateException(
                    "IEMA respondeu com corpo vazio - confira os headers User-Agent e Referer");
        }
        return Json.mapper().readTree(resp.body());
    }

    /** Devolve null em vez de "null" textual, para o campo chegar nulo ao evento. */
    private static String texto(JsonNode no, String campo) {
        JsonNode v = no.path(campo);
        return (v.isMissingNode() || v.isNull()) ? null : v.asText();
    }

    /** A API usa 0001-01-01T00:00:00 como sentinela de "sem medicao". */
    private static LocalDateTime dataHora(JsonNode no) {
        if (no.isMissingNode() || no.isNull()) {
            return null;
        }
        try {
            return LocalDateTime.parse(no.asText());
        } catch (Exception e) {
            return null;
        }
    }
}
