package io.datalatte.etl.transformer;

import io.datalatte.etl.transformer.NoopTransformer;
import io.datalatte.etl.transformer.Transformer;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.*;

class NoopTransformerTest {

    @Test
    void returnsSameReference_andEqualContent() {
        Transformer<Map<String,Object>, Map<String,Object>> tx = new NoopTransformer<>();
        Map<String,Object> input = new HashMap<>();
        input.put("id", 1);
        input.put("first_name", "Ada");
        input.put("last_name", "Lovelace");

        Map<String,Object> output = tx.apply(input);

        // identity pass-through
        assertSame(input, output);
        // and obviously equal by content
        assertEquals(input, output);
    }
}