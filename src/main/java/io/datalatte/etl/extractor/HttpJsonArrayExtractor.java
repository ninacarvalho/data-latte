package io.datalatte.etl.extractor;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

/**
 * An implementation of {@link Extractor} that fetches a JSON array from an HTTP endpoint.
 * This is a Java {@code record}, which is a concise way to create immutable data-carrier classes.
 *
 * @param rest The {@link RestTemplate} to use for making the HTTP request.
 * @param url  The URL of the HTTP endpoint to fetch data from.
 */
public record HttpJsonArrayExtractor(RestTemplate rest, String url)
        implements Extractor<JsonNode> {

    // A type reference used to tell RestTemplate how to deserialize the JSON response into a JsonNode.
    private static final ParameterizedTypeReference<JsonNode> JSON_NODE =
            new ParameterizedTypeReference<>() {};

    /**
     * Fetches all records from the configured URL.
     * It expects the response to be a JSON array.
     *
     * @return An {@link Iterable} of {@link JsonNode}s, where each node is an element of the JSON array.
     *         Returns an empty list if the response body is null or not a JSON array.
     */
    @Override
    public Iterable<JsonNode> fetchAll() {
        // Make an HTTP GET request to the specified URL.
        ResponseEntity<JsonNode> resp =
                rest.exchange(url, HttpMethod.GET, null, JSON_NODE);

        // Get the response body.
        JsonNode body = resp.getBody();

        // If the body is null or not a JSON array, return an empty list to prevent errors downstream.
        if (body == null || !body.isArray()) {
            return java.util.List.of();
        }

        // The body is an iterable JSON array, so it can be returned directly.
        return body;
    }
}
