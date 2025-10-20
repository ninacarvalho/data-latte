package io.datalatte.etl.extractor;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class FakeExtractor implements Extractor<Map<String,Object>> {
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final String[] resourcePaths;

    public FakeExtractor(String... resourcePaths) {
        this.resourcePaths = resourcePaths;
    }

    @Override
    public List<Map<String, Object>> fetchAll() {
        List<Map<String,Object>> out = new ArrayList<>();
        for (String path : resourcePaths) {
            try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(path)) {
                if (is == null) throw new IllegalArgumentException("Resource not found: " + path);
                List<Map<String,Object>> page = MAPPER.readValue(is, new TypeReference<>() {});
                out.addAll(page);
            } catch (Exception e) {
                throw new RuntimeException("Failed to load fixture: " + path, e);
            }
        }
        return out;
    }
}
