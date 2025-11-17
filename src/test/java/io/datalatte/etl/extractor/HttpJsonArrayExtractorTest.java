package io.datalatte.etl.extractor;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;

import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.stream.StreamSupport;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.client.ExpectedCount.once;
import static org.springframework.test.web.client.match.MockRestRequestMatchers.requestTo;
import static org.springframework.test.web.client.response.MockRestResponseCreators.withSuccess;

class HttpJsonArrayExtractorTest {
    private static final String URL = "https://api.github.com/events";
    private static final ObjectMapper M = new ObjectMapper();

    @Test
    void returnsListOfMaps_whenApiRespondsArray() throws Exception {
        RestTemplate rt = new RestTemplate();
        MockRestServiceServer server = MockRestServiceServer.bindTo(rt).build();

        String body = new String(
                Thread.currentThread().getContextClassLoader()
                        .getResourceAsStream("fixtures/github_events_page1.json")
                        .readAllBytes(),
                StandardCharsets.UTF_8);

        server.expect(once(), requestTo(URL))
                .andRespond(withSuccess(body, MediaType.APPLICATION_JSON));

        HttpJsonArrayExtractor ex = new HttpJsonArrayExtractor(rt, URL);

        Iterable<JsonNode> raw = ex.fetchAll();

        List<JsonNode> out = StreamSupport.stream(raw.spliterator(), false).toList();

        JsonNode first = out.get(0);

        assertThat(first.has("id")).isTrue();
        assertThat(first.has("type")).isTrue();
        assertThat(first.has("actor")).isTrue();
        assertThat(first.has("repo")).isTrue();
        assertThat(first.has("created_at")).isTrue();

        server.verify();
    }
}
