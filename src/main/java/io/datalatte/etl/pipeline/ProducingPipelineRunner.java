package io.datalatte.etl.pipeline;

import io.datalatte.etl.extractor.Extractor;
import io.datalatte.etl.producer.Producer;
import io.datalatte.etl.transformer.Transformer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * Extends PipelineRunner with a load stage that sends transformed results to a Producer (e.g. Kafka).
 */
public class ProducingPipelineRunner<I, O> extends PipelineRunner<I, O> {

    private static final Logger log = LoggerFactory.getLogger(ProducingPipelineRunner.class);

    private final Producer<O> producer;

    public ProducingPipelineRunner(Extractor<I> extractor,
                                   Transformer<I, O> transformer,
                                   Producer<O> producer) {
        super(extractor, transformer);
        this.producer = producer;
    }

    @Override
    public void runOnce() {
        List<O> out = runOnceAndReturn();

        if (out.isEmpty()) {
            log.info("etl_run stage=produce count=0 (no records)");
            return;
        }

        producer.sendBatch(out);
        log.info("etl_run stage=produce count={}", out.size());
    }
}
