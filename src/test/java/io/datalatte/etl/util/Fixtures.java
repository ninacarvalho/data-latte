package io.datalatte.etl.util;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;

public final class Fixtures {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private Fixtures() {}

    public static List<Map<String,Object>> readJsonArrayAsListOfMaps(String path) {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path)) {
            if (is == null) throw new IllegalArgumentException("fixture not found: " + path);
            return MAPPER.readValue(is, new TypeReference<List<Map<String,Object>>>(){});
        } catch (Exception e) {
            throw new RuntimeException("failed reading fixture: " + path, e);
        }
    }

    public static String readText(String path) {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path)) {
            if (is == null) throw new IllegalArgumentException("fixture not found: " + path);
            return new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new RuntimeException("failed reading fixture: " + path, e);
        }
    }
}
