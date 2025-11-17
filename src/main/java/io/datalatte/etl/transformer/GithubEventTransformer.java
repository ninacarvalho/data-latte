package io.datalatte.etl.transformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import io.datalatte.etl.model.GithubEvent;
import java.util.Map;

public class GithubEventTransformer implements Transformer<JsonNode, GithubEvent> {

    @Override
    public GithubEvent apply(JsonNode in) {
        if (in == null) throw new IllegalArgumentException("input is null");

        String id = nodeToString(in.get("id"));
        String type = nodeToString(in.get("type"));

        if (id.isBlank() || type.isBlank()) {
            throw new IllegalArgumentException("id and type required");
        }

        JsonNode actor = textOrEmpty(in.get("actor"));
        JsonNode repo  = textOrEmpty(in.get("repo"));
        JsonNode org   = textOrEmpty(in.get("org"));

        return GithubEvent.builder()
                .id(id)
                .eventType(type)
                .actor(nodeToString(actor.get("login")))
                .repo(nodeToString(repo.get("name")))
                .org(org.isEmpty() ? null : nodeToString(org.get("login")))
                .createdAt(nodeToString(in.get("created_at")))
                .isPublic(nodeToBoolean(in.get("public")))
                .build();
    }

    // nullpointer prevention
    private JsonNode textOrEmpty(JsonNode node) {
        return (node != null && node.isObject()) ? node : NullNode.getInstance();
    }

    private static String nodeToString(JsonNode node) {
        if (node == null || node.isNull()) return "";
        String rawText = node.asText();
        return rawText == null ? "" : rawText.trim();
    }

    private static boolean nodeToBoolean(JsonNode node) {
        if (node == null || node.isNull()) return false;
        if (node.isBoolean()) return node.booleanValue();
        if (node.isNumber()) return node.intValue() != 0;
        if (node.isTextual()) {
            String s = node.asText();
            return "true".equalsIgnoreCase(s) || "1".equals(s);
        }    return false;
    }
}
