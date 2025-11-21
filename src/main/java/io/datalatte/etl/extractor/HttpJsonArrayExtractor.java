package io.datalatte.etl.extractor;

import com.fasterxml.jackson.databind.JsonNode;
import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

public record HttpJsonArrayExtractor(RestTemplate rest, String url)
        implements Extractor<JsonNode> {

    private static final ParameterizedTypeReference<JsonNode> JSON_NODE =
            new ParameterizedTypeReference<>() {};

    @Override
    public Iterable<JsonNode> fetchAll() {
        ResponseEntity<JsonNode> resp =
                rest.exchange(url, HttpMethod.GET, null, JSON_NODE);

        JsonNode body = resp.getBody();

        if (body == null || !body.isArray()) {
            return java.util.List.of();
        }

        return body;
    }
}
