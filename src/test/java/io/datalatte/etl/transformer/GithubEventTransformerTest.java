package io.datalatte.etl.transformer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.datalatte.etl.model.GithubEvent;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GithubEventTransformerTest {
    private static final ObjectMapper M = new ObjectMapper();

    @Test
    void maps_sample_events_exactly() throws Exception {
        List<Map<String,Object>> raw = readMaps("fixtures/github_events_page1.json");
        List<Map<String,Object>> expected = readMaps("fixtures/expected_github_events_transformed.json");

        Transformer<Map<String,Object>, GithubEvent> tx = new GithubEventTransformer();
        List<Map<String,Object>> actual = raw.stream()
                .map(tx::apply)
                .map(GithubEventTransformerTest::toExpectedShape) // model -> map with keys matching fixture
                .toList();

        assertEquals(expected, actual);
    }

    private static List<Map<String,Object>> readMaps(String p) throws Exception {
        try (InputStream is = Thread.currentThread().getContextClassLoader().getResourceAsStream(p)) {
            return M.readValue(is, new TypeReference<>() {});
        }
    }

    private static Map<String,Object> toExpectedShape(GithubEvent e) {
        Map<String,Object> m = new LinkedHashMap<>();
        m.put("id", e.getId());
        m.put("eventType", e.getEventType());
        m.put("actor", e.getActor());
        m.put("repo", e.getRepo());
        m.put("org", e.getOrg());              // may be null
        m.put("createdAt", e.getCreatedAt());
        m.put("public", e.isPublic());         // key name matches expected JSON
        return m;
    }
}
