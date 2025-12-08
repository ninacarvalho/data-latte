package io.datalatte.etl.pipeline;

import io.datalatte.etl.extractor.Extractor;
import io.datalatte.etl.producer.Producer;
import io.datalatte.etl.transformer.Transformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A specialized {@link PipelineRunner} that adds a "producing" step to the ETL process.
 * After extracting and transforming the data, this runner sends the results to a {@link Producer}.
 *
 * @param <I> The type of the input data from the extractor.
 * @param <O> The type of the output data from the transformer, which will be sent to the producer.
 */
public class ProducingPipelineRunner<I, O> extends PipelineRunner<I, O> {

    private static final Logger log = LoggerFactory.getLogger(ProducingPipelineRunner.class);

    private final Producer<O> producer;

    /**
     * Constructs a new ProducingPipelineRunner.
     *
     * @param extractor   The component for extracting data.
     * @param transformer The component for transforming data.
     * @param producer    The component for producing (sending) the transformed data to a destination like Kafka.
     */
    public ProducingPipelineRunner(Extractor<I> extractor,
                                   Transformer<I, O> transformer,
                                   Producer<O> producer) {
        super(extractor, transformer);
        this.producer = producer;
    }

    /**
     * Executes the full ETL and produce pipeline once.
     * It first runs the extraction and transformation steps, and then sends the resulting data
     * as a batch to the configured producer.
     */
    @Override
    public void runOnce() {
        // Run the parent's ETL process to get the list of transformed items.
        List<O> out = runOnceAndReturn();

        // If there are no records to produce, log it and exit.
        if (out.isEmpty()) {
            log.info("etl_run stage=produce count=0 (no records)");
            return;
        }

        // Send the batch of transformed records to the producer (e.g., Kafka).
        producer.sendBatch(out);
        log.info("etl_run stage=produce count={}", out.size());
    }
}
