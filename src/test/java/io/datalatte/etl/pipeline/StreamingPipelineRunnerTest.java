package io.datalatte.etl.pipeline;

import io.datalatte.etl.extractor.Extractor;
import io.datalatte.etl.transformer.Transformer;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;

import static org.junit.jupiter.api.Assertions.assertEquals;

class StreamingPipelineRunnerTest {

    @Test
    void processRecords_pushes_each_item_incrementally() {
        // extractor that returns a simple iterable
        Extractor<String> extractor = () -> List.of("a", "b", "c");
        Transformer<String, String> transformer = String::toUpperCase;

        PipelineRunner<String, String> runner = new PipelineRunner<>(extractor, transformer);

        List<String> sink = new ArrayList<>();
        AtomicInteger outCount = new AtomicInteger();

      int processed = runner.processRecords(o -> {
            sink.add(o);
            outCount.incrementAndGet();
        });

        assertEquals(3, processed);
        assertEquals(3, outCount.get());
        assertEquals(List.of("A", "B", "C"), sink);
    }
}
