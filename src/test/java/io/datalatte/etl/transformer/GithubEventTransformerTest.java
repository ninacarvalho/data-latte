package io.datalatte.etl.transformer;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import io.datalatte.etl.model.GithubEvent;
import org.junit.jupiter.api.Test;

import java.io.InputStream;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;

class GithubEventTransformerTest {
    private static final ObjectMapper M = new ObjectMapper();

    @Test
    void maps_sample_events_exactly() throws Exception {
        List<JsonNode> raw = readNodes("fixtures/github_events_page1.json");
        List<JsonNode> expected = readNodes("fixtures/expected_github_events_transformed.json");

        Transformer<JsonNode, GithubEvent> tx = new GithubEventTransformer();

        List<JsonNode> actual = raw.stream()
                .map(tx::apply)
                .map(GithubEventTransformerTest::toExpectedShape)
                .toList();

        assertEquals(expected, actual);
    }

    private static List<JsonNode> readNodes(String path) throws Exception {
        try (InputStream is = Thread.currentThread()
                .getContextClassLoader()
                .getResourceAsStream(path)) {
            return M.readValue(is, new TypeReference<List<JsonNode>>() {});
        }
    }

    public static JsonNode toExpectedShape(GithubEvent e) {
        ObjectNode node = M.createObjectNode();
        node.put("id", e.getId());
        node.put("eventType", e.getEventType());
        node.put("actor", e.getActor());
        node.put("repo", e.getRepo());

        if (e.getOrg() == null) {
            node.putNull("org");
        } else {
            node.put("org", e.getOrg());
        }
        node.put("createdAt", e.getCreatedAt());
        node.put("public", e.isPublic());

        return node;
    }
}
