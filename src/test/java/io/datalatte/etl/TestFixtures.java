package io.datalatte.etl;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public final class TestFixtures {
    private static final ObjectMapper MAPPER = new ObjectMapper();

    public static String loadText(String path) throws Exception {
        try (InputStream is = resource(path)) {
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    public static List<Map<String, Object>> loadListOfMaps(String path) throws Exception {
        try (InputStream is = resource(path)) {
            return MAPPER.readValue(is, new TypeReference<>() {});
        }
    }

    private static InputStream resource(String path) throws Exception {
        var url = Thread.currentThread().getContextClassLoader().getResource(path);
        if (url == null) throw new IllegalArgumentException("Resource not found: " + path);
        return url.openStream();
    }

}
