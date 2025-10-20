package io.datalatte.etl.pipeline;

import io.datalatte.etl.extractor.Extractor;
import io.datalatte.etl.transformer.Transformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Minimal pipeline runner (no framework).
 * Extracts all records, transforms each, returns transformed list.
 */
public class PipelineRunner<I, O> implements Pipeline {
    private static final Logger log = LoggerFactory.getLogger(PipelineRunner.class);

    private final Extractor<I> extractor;
    private final Transformer<I, O> transformer;

    public PipelineRunner(Extractor<I> extractor, Transformer<I, O> transformer) {
        this.extractor = extractor;
        this.transformer = transformer;
    }

    /** Execute one ETL pass and return transformed results. */
    public List<O> runOnceAndReturn() {
        List<I> raw = extractor.fetchAll();
        int inCount = raw.size();
        List<O> out = new ArrayList<>(inCount);
        for (I item : raw) {
            out.add(transformer.apply(item));
        }
        log.info("etl_run stage=extract count={}", inCount);
        log.info("etl_run stage=transform count={}", out.size());
        return out;
    }

    @Override
    public void runOnce() {
        runOnceAndReturn();
    }
}
