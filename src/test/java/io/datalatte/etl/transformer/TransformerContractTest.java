package io.datalatte.etl.transformer;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import java.util.List;
import java.util.Map;

import static io.datalatte.etl.util.Fixtures.readJsonArrayAsListOfMaps;


/**
 * Defines expected shape AFTER transformation.
 * Will fail until Transformer implementation exists.
 */
class TransformerContractTest {

    @Test
    void maps_users_pages_to_expected_shape() {
        // arrange fixtures
        List<Map<String,Object>> p1 = readJsonArrayAsListOfMaps("fixtures/users_page1.json");
        List<Map<String,Object>> p2 = readJsonArrayAsListOfMaps("fixtures/users_page2.json");
        List<Map<String,Object>> expected = readJsonArrayAsListOfMaps("fixtures/expected_users_transformed.json");

        Transformer<Map<String,Object>, Map<String,Object>> tx = new FakeUserTransformer();

        // act: apply transform for all inputs
        List<Map<String,Object>> actual = java.util.stream.Stream.concat(p1.stream(), p2.stream())
                .map(tx::apply)
                .toList();

        // assert: deep equals expected (order matters by id asc)
        assertEquals(expected, actual);
    }
}
