package br.ufes.soe.ar.infra;

import java.io.InputStream;
import java.util.Properties;

/**
 * Le {@code config.properties} do classpath. Qualquer chave pode ser sobrescrita
 * na linha de comando com {@code -Dchave=valor}, o que e util para ajustar limiares
 * durante a apresentacao sem recompilar.
 */
public final class Config {

    private static final Properties PROPS = carregar();

    private Config() {
    }

    private static Properties carregar() {
        Properties p = new Properties();
        try (InputStream in = Config.class.getResourceAsStream("/config.properties")) {
            if (in == null) {
                throw new IllegalStateException("config.properties nao encontrado no classpath");
            }
            p.load(in);
        } catch (Exception e) {
            throw new IllegalStateException("Falha ao ler config.properties", e);
        }
        return p;
    }

    public static String texto(String chave) {
        String valor = System.getProperty(chave, PROPS.getProperty(chave));
        if (valor == null) {
            throw new IllegalArgumentException("Chave de configuracao ausente: " + chave);
        }
        return valor;
    }

    public static int inteiro(String chave) {
        return Integer.parseInt(texto(chave).trim());
    }

    public static double decimal(String chave) {
        return Double.parseDouble(texto(chave).trim());
    }

    public static String bootstrap() {
        return texto("kafka.bootstrap");
    }
}
