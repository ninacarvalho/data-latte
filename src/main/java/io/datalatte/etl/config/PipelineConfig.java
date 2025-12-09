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

/**
 * Spring configuration class for setting up the ETL pipeline.
 * This class defines the beans that constitute the data processing pipeline.
 */
@Configuration
public class PipelineConfig {

    /**
     * Creates and configures the main ETL pipeline bean.
     *
     * @param restTemplate The RestTemplate for making HTTP requests.
     * @param producer     The Kafka producer for sending data to a Kafka topic.
     * @param apiUrl       The URL of the API to extract data from, injected from application properties.
     * @return A configured {@link Pipeline} instance.
     */
    @Bean
    public Pipeline pipeline(
            RestTemplate restTemplate,
            KafkaEventProducer producer,
            @Value("${app.api.url}") String apiUrl) {

        // 1. The Extractor: Fetches raw data from the GitHub Events API.
        var extractor = new HttpJsonArrayExtractor(restTemplate, apiUrl);

        // 2. The Transformer: Converts the raw JSON data into structured GithubEvent objects.
        var transformer = new GithubEventTransformer();

        // 3. The Pipeline Runner: Orchestrates the process, connecting the extractor, transformer,
        //    and producer. The ProducingPipelineRunner is a specialized runner that sends the
        //    transformed data to the provided producer (in this case, Kafka).
        return new ProducingPipelineRunner<>(extractor, transformer, producer);
    }
}
