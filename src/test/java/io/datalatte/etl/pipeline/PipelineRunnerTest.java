package io.datalatte.etl.pipeline;

import io.datalatte.etl.extractor.Extractor;
import io.datalatte.etl.transformer.Transformer;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.*;

class PipelineRunnerTest {

    @Test
    void callsExtractor_thenTransforms_allRecords_andReturnsOutput() {
        // arrange
        @SuppressWarnings("unchecked")
        Extractor<Map<String,Object>> extractor = mock(Extractor.class);
        @SuppressWarnings("unchecked")
        Transformer<Map<String,Object>, Map<String,Object>> transformer = mock(Transformer.class);

        List<Map<String,Object>> raw = List.of(
                Map.of("id", 2, "first_name", "Alan"),
                Map.of("id", 1, "first_name", "Ada")
        );
        when(extractor.fetchAll()).thenReturn(raw);
        when(transformer.apply(raw.get(0))).thenReturn(Map.of("id", 1, "fullName", "Ada"));
        when(transformer.apply(raw.get(1))).thenReturn(Map.of("id", 2, "fullName", "Alan"));

        PipelineRunner<Map<String,Object>, Map<String,Object>> runner = new PipelineRunner<>(extractor, transformer);

        // act
        var out = runner.runOnceAndReturn();

        // assert output
        assertThat(out).containsExactly(
                Map.of("id", 1, "fullName", "Ada"),
                Map.of("id", 2, "fullName", "Alan")
        );

        // verify order: extractor first, then transformer for each record
        InOrder inOrder = inOrder(extractor, transformer);
        inOrder.verify(extractor, times(1)).fetchAll();
        inOrder.verify(transformer).apply(raw.get(0));
        inOrder.verify(transformer).apply(raw.get(1));
        verifyNoMoreInteractions(extractor, transformer);
    }
}