package io.datalatte.etl.config;

import io.datalatte.etl.extractor.HttpJsonArrayExtractor;
import io.datalatte.etl.pipeline.Pipeline;
import io.datalatte.etl.pipeline.ProducingPipelineRunner;
import io.datalatte.etl.producer.KafkaEventProducer;
import io.datalatte.etl.transformer.GithubEventTransformer;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.client.RestTemplate;

@Configuration
public class PipelineConfig {

    @Bean
    public Pipeline pipeline(
            RestTemplate restTemplate,
            KafkaEventProducer producer,
            @Value("${app.api.url}") String apiUrl) {

        var extractor = new HttpJsonArrayExtractor(restTemplate, apiUrl);
        var transformer = new GithubEventTransformer();
        
        return new ProducingPipelineRunner<>(extractor, transformer, producer);
    }
}
