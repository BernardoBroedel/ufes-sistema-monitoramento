package br.ufes.soe.ar.serde;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

/** ObjectMapper compartilhado, configurado uma vez so. */
public final class Json {

    private static final ObjectMapper MAPPER = new ObjectMapper()
            // Instant e LocalDateTime como texto ISO-8601, nao como numero
            .registerModule(new JavaTimeModule())
            .disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS)
            // campos novos na API nao quebram o consumidor
            .disable(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES);

    private Json() {
    }

    public static ObjectMapper mapper() {
        return MAPPER;
    }
}
