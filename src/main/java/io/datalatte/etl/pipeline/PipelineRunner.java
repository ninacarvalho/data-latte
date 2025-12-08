package io.datalatte.etl.pipeline;

import io.datalatte.etl.extractor.Extractor;
import io.datalatte.etl.transformer.Transformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

/**
 * A generic implementation of the {@link Pipeline} interface that orchestrates the ETL process.
 * It uses an {@link Extractor} to fetch data and a {@link Transformer} to process it.
 *
 * @param <I> The type of the input data from the extractor.
 * @param <O> The type of the output data from the transformer.
 */
public class PipelineRunner<I, O> implements Pipeline {
    private static final Logger log = LoggerFactory.getLogger(PipelineRunner.class);

    private final Extractor<I> extractor;
    private final Transformer<I, O> transformer;

    /**
     * Constructs a new PipelineRunner with the specified extractor and transformer.
     *
     * @param extractor The component responsible for extracting data.
     * @param transformer The component responsible for transforming data.
     */
    public PipelineRunner(Extractor<I> extractor, Transformer<I, O> transformer) {
        this.extractor = extractor;
        this.transformer = transformer;
    }

    /**
     * Runs the ETL process once and collects the transformed results into a list.
     *
     * @return A list of transformed objects.
     */
    public List<O> runOnceAndReturn() {
        List<O> out = new ArrayList<>();
        processRecords(out::add);
        return out;
    }

    /**
     * Runs the ETL process once without returning the results.
     * This is the implementation of the {@link Pipeline#runOnce()} method.
     */
    @Override
    public void runOnce() {
        processRecords(null);
    }

    /**
     * The core logic of the ETL process. It fetches records from the extractor,
     * transforms them, and then passes them to a consumer (sink).
     *
     * @param sink A consumer that accepts the transformed records. If null, records are transformed but not consumed.
     * @return The number of records successfully transformed.
     */
    protected int processRecords(Consumer<O> sink) {
        // Step 1: Extract raw data.
        Iterable<I> raw = extractor.fetchAll();
        if (raw == null) {
            raw = Collections.emptyList();
        }

        int inCount = 0; // Counter for records seen from the extractor.
        int outCount = 0; // Counter for records successfully transformed.

        // Step 2: Transform each item.
        for (I item : raw) {
            inCount++;
            O transformed = transformer.apply(item);
            outCount++;
            // Step 3: Load the transformed item into the sink, if provided.
            if (sink != null) {
                sink.accept(transformed);
            }
        }

        // Log the results of the ETL run.
        log.info("etl_run stage=extract count={}", inCount);
        log.info("etl_run stage=transform count={}", outCount);
        return outCount;
    }
}
