package br.ufes.soe.ar.consumidor;

import br.ufes.soe.ar.infra.Config;
import br.ufes.soe.ar.infra.Topicos;
import br.ufes.soe.ar.modelo.Alerta;
import br.ufes.soe.ar.modelo.CondicaoClima;
import br.ufes.soe.ar.modelo.Medicao;
import br.ufes.soe.ar.serde.Json;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;

/**
 * NOTIFICADOR — atende a "funcionalidade desejavel" do enunciado: exibir as
 * situacoes de interesse detectadas.
 *
 * <p>Entrega as duas formas previstas na secao 10 da proposta:
 * <ul>
 *   <li><b>CSV</b> em {@code saida/alertas.csv}, que abre direto no Excel. E a
 *       garantia: se o painel falhar na apresentacao, o requisito continua atendido.</li>
 *   <li><b>Painel web</b> em {@code http://localhost:8080}, servido pelo
 *       {@code com.sun.net.httpserver.HttpServer} — que ja vem no JDK, entao nao
 *       entra nenhuma dependencia nova no pom.</li>
 * </ul>
 *
 * <p>Duas threads consomem em paralelo: uma le {@code ar.alertas} (o que alimenta
 * o CSV e a linha do tempo) e outra le {@code ar.medicoes} e {@code clima.condicoes}
 * (o que alimenta os cartoes das estacoes). A thread do servidor HTTP so le o
 * estado compartilhado.
 *
 * <p>Uso: {@code java -cp target/monitoramento-ar.jar br.ufes.soe.ar.consumidor.Notificador}
 */
public class Notificador {

    private static final EstadoPainel ESTADO = new EstadoPainel();

    public static void main(String[] args) throws Exception {
        Path csv = Path.of(Config.texto("notificador.csv"));
        int porta = Config.inteiro("notificador.http.porta");

        prepararCsv(csv);
        subirServidor(porta);

        Thread tAlertas = new Thread(() -> consumirAlertas(csv), "consumidor-alertas");
        Thread tMedicoes = new Thread(Notificador::consumirMedicoes, "consumidor-medicoes");
        Thread tClima = new Thread(Notificador::consumirClima, "consumidor-clima");
        tAlertas.start();
        tMedicoes.start();
        tClima.start();

        System.out.println("notificador no ar");
        System.out.println("  painel : http://localhost:" + porta);
        System.out.println("  csv    : " + csv.toAbsolutePath());
        System.out.println("Ctrl+C para encerrar.\n");

        tAlertas.join();
    }

    // ------------------------------------------------------------------ CSV

    private static void prepararCsv(Path csv) throws IOException {
        if (csv.getParent() != null) {
            Files.createDirectories(csv.getParent());
        }
        if (!Files.exists(csv)) {
            Files.writeString(csv, Alerta.cabecalhoCsv() + System.lineSeparator(),
                    StandardCharsets.UTF_8, StandardOpenOption.CREATE);
        }
    }

    private static void anexarCsv(Path csv, Alerta a) {
        try {
            Files.writeString(csv, a.paraCsv() + System.lineSeparator(),
                    StandardCharsets.UTF_8, StandardOpenOption.APPEND);
        } catch (IOException e) {
            System.err.println("falha ao gravar no CSV: " + e.getMessage());
        }
    }

    // ------------------------------------------------------------ consumidores

    private static void consumirAlertas(Path csv) {
        try (KafkaConsumer<String, Alerta> c =
                     Kafka.consumidor("notificador-alertas", Alerta.class, Topicos.ALERTAS)) {
            while (true) {
                ConsumerRecords<String, Alerta> registros = c.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, Alerta> r : registros) {
                    Alerta a = r.value();
                    if (a == null) {
                        continue;
                    }
                    ESTADO.registrarAlerta(a);
                    anexarCsv(csv, a);
                    System.out.printf("  [%s] %s - %s%n",
                            a.severidade(), a.tipo(), a.localizacao());
                }
            }
        }
    }

    private static void consumirMedicoes() {
        try (KafkaConsumer<String, Medicao> c =
                     Kafka.consumidor("notificador-medicoes", Medicao.class, Topicos.MEDICOES)) {
            while (true) {
                for (ConsumerRecord<String, Medicao> r : c.poll(Duration.ofMillis(1000))) {
                    if (r.value() != null) {
                        ESTADO.registrarMedicao(r.value());
                    }
                }
            }
        }
    }

    private static void consumirClima() {
        try (KafkaConsumer<String, CondicaoClima> c =
                     Kafka.consumidor("notificador-clima", CondicaoClima.class, Topicos.CLIMA)) {
            while (true) {
                for (ConsumerRecord<String, CondicaoClima> r : c.poll(Duration.ofMillis(1000))) {
                    if (r.value() != null) {
                        ESTADO.registrarClima(r.value());
                    }
                }
            }
        }
    }

    // ------------------------------------------------------------------ HTTP

    private static void subirServidor(int porta) throws IOException {
        HttpServer servidor = HttpServer.create(new InetSocketAddress(porta), 0);

        servidor.createContext("/api/estado", troca -> {
            byte[] corpo = Json.mapper().writeValueAsBytes(ESTADO.instantaneo());
            responder(troca, 200, "application/json; charset=utf-8", corpo);
        });

        servidor.createContext("/", troca -> {
            if (!"/".equals(troca.getRequestURI().getPath())) {
                responder(troca, 404, "text/plain; charset=utf-8",
                        "nao encontrado".getBytes(StandardCharsets.UTF_8));
                return;
            }
            try (InputStream in = Notificador.class.getResourceAsStream("/painel.html")) {
                byte[] html = in == null
                        ? "painel.html ausente do jar".getBytes(StandardCharsets.UTF_8)
                        : in.readAllBytes();
                responder(troca, 200, "text/html; charset=utf-8", html);
            }
        });

        servidor.setExecutor(null);
        servidor.start();
    }

    private static void responder(HttpExchange troca, int status, String tipo, byte[] corpo)
            throws IOException {
        troca.getResponseHeaders().set("Content-Type", tipo);
        troca.getResponseHeaders().set("Cache-Control", "no-store");
        troca.sendResponseHeaders(status, corpo.length);
        try (OutputStream out = troca.getResponseBody()) {
            out.write(corpo);
        }
    }
}
