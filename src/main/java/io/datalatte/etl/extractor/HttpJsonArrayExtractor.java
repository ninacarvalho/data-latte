package io.datalatte.etl.extractor;

import org.springframework.core.ParameterizedTypeReference;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.web.client.RestTemplate;

import java.util.List;
import java.util.Map;

public record HttpJsonArrayExtractor(RestTemplate rest, String url)
        implements Extractor<Map<String,Object>> {

    private static final ParameterizedTypeReference<List<Map<String,Object>>> LIST_OF_MAP =
            new ParameterizedTypeReference<>() {};

    @Override
    public List<Map<String,Object>> fetchAll() {
        ResponseEntity<List<Map<String,Object>>> resp =
                rest.exchange(url, HttpMethod.GET, null, LIST_OF_MAP);
        List<Map<String,Object>> body = resp.getBody();
        return body == null ? List.of() : body;
    }
}
