package br.ufes.soe.ar.infra;

import br.ufes.soe.ar.modelo.CondicaoClima;
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

/**
 * Cliente do Open-Meteo: vento e chuva da Grande Vitoria. Sem chave e sem cadastro.
 *
 * <p>E a segunda fonte de eventos primitivos. Sozinha nao gera alerta; existe para
 * ser correlacionada com as medicoes de ar na funcionalidade F4.
 */
public class ClienteOpenMeteo {

    private static final String BASE = "https://api.open-meteo.com/v1/forecast";

    private final HttpClient http = HttpClient.newBuilder()
            .connectTimeout(Duration.ofSeconds(15))
            .build();

    private final String url = BASE
            + "?latitude=" + Config.texto("clima.latitude")
            + "&longitude=" + Config.texto("clima.longitude")
            + "&current=temperature_2m,precipitation,wind_speed_10m,wind_direction_10m"
            + "&timezone=America%2FSao_Paulo";

    /**
     * Serie horaria dos ultimos dias, usada pelo produtor-replay.
     *
     * <p>Sem isto o replay ficaria incoerente: as medicoes viriam de dois dias atras
     * e o clima, de agora. O correlacionador de F4 nao acharia condicao meteorologica
     * nenhuma dentro da janela e o evento derivado nunca seria inferido.
     */
    public List<CondicaoClima> lerHistorico(int dias) throws Exception {
        String urlHist = BASE
                + "?latitude=" + Config.texto("clima.latitude")
                + "&longitude=" + Config.texto("clima.longitude")
                + "&hourly=temperature_2m,precipitation,wind_speed_10m,wind_direction_10m"
                + "&past_days=" + dias + "&forecast_days=1"
                + "&timezone=America%2FSao_Paulo";

        HttpRequest req = HttpRequest.newBuilder(URI.create(urlHist))
                .header("Accept", "application/json")
                .timeout(Duration.ofSeconds(40))
                .GET()
                .build();

        HttpResponse<byte[]> resp = http.send(req, HttpResponse.BodyHandlers.ofByteArray());
        if (resp.statusCode() != 200) {
            throw new IllegalStateException("Open-Meteo respondeu HTTP " + resp.statusCode());
        }

        JsonNode h = Json.mapper().readTree(resp.body()).path("hourly");
        JsonNode tempos = h.path("time");
        Instant agora = Instant.now();
        List<CondicaoClima> serie = new ArrayList<>();

        for (int i = 0; i < tempos.size(); i++) {
            serie.add(new CondicaoClima(
                    LocalDateTime.parse(tempos.get(i).asText()),
                    h.path("temperature_2m").path(i).asDouble(),
                    h.path("precipitation").path(i).asDouble(),
                    h.path("wind_speed_10m").path(i).asDouble(),
                    h.path("wind_direction_10m").path(i).asInt(),
                    agora));
        }
        return serie;
    }

    public CondicaoClima lerCondicaoAtual() throws Exception {
        HttpRequest req = HttpRequest.newBuilder(URI.create(url))
                .header("Accept", "application/json")
                .timeout(Duration.ofSeconds(30))
                .GET()
                .build();

        HttpResponse<byte[]> resp = http.send(req, HttpResponse.BodyHandlers.ofByteArray());
        if (resp.statusCode() != 200) {
            throw new IllegalStateException("Open-Meteo respondeu HTTP " + resp.statusCode());
        }

        JsonNode atual = Json.mapper().readTree(resp.body()).path("current");
        return new CondicaoClima(
                LocalDateTime.parse(atual.path("time").asText()),
                atual.path("temperature_2m").asDouble(),
                atual.path("precipitation").asDouble(),
                atual.path("wind_speed_10m").asDouble(),
                atual.path("wind_direction_10m").asInt(),
                Instant.now());
    }
}
