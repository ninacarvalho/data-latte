package io.datalatte.etl.transformer;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.node.NullNode;
import io.datalatte.etl.model.GithubEvent;

/**
 * Transforms a raw JSON representation of a GitHub event into a structured {@link GithubEvent} object.
 */
public class GithubEventTransformer implements Transformer<JsonNode, GithubEvent> {

    /**
     * Transforms a single JSON node into a {@link GithubEvent}.
     *
     * @param in The input JSON node representing a GitHub event.
     * @return A {@link GithubEvent} object.
     * @throws IllegalArgumentException if the input is null or missing required fields.
     */
    @Override
    public GithubEvent apply(JsonNode in) {
        if (in == null) throw new IllegalArgumentException("input is null");

        // Extract mandatory fields: id and type.
        String id = nodeToString(in.get("id"));
        String type = nodeToString(in.get("type"));

        // Validate that mandatory fields are present.
        if (id.isBlank() || type.isBlank()) {
            throw new IllegalArgumentException("id and type required");
        }

        // Safely access nested JSON objects.
        JsonNode actor = objectOrEmpty(in.get("actor"));
        JsonNode repo  = objectOrEmpty(in.get("repo"));
        JsonNode org   = objectOrEmpty(in.get("org"));

        // Build the GithubEvent object using extracted and transformed values.
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

    /**
     * Safely handles nested JSON objects to prevent NullPointerExceptions.
     * If the node is null or not an object, it returns a {@link NullNode} instance,
     * which is a safe, empty node.
     *
     * @param node The JSON node to check.
     * @return The original node if it's a valid object, otherwise a {@link NullNode}.
     */
    private JsonNode objectOrEmpty(JsonNode node) {
        return (node != null && node.isObject()) ? node : NullNode.getInstance();
    }

    /**
     * Converts a JSON node to a trimmed string, returning an empty string if the node is null or doesn't contain text.
     *
     * @param node The JSON node to convert.
     * @return The trimmed string value or an empty string.
     */
    private static String nodeToString(JsonNode node) {
        if (node == null || node.isNull()) return "";
        String rawText = node.asText();
        return rawText == null ? "" : rawText.trim();
    }

    /**
     * Converts a JSON node to a boolean value. It handles boolean, numeric, and textual representations of booleans.
     *
     * @param node The JSON node to convert.
     * @return The boolean value, or false if the node is null or cannot be converted.
     */
    private static boolean nodeToBoolean(JsonNode node) {
        if (node == null || node.isNull()) return false;
        if (node.isBoolean()) return node.booleanValue();
        if (node.isNumber()) return node.intValue() != 0;
        if (node.isTextual()) {
            String s = node.asText();
            return "true".equalsIgnoreCase(s) || "1".equals(s);
        }
        return false;
    }
}
