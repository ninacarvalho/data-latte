package io.datalatte.etl.extractor;

import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import static org.junit.jupiter.api.Assertions.*;

class ExtractorContractTest {

    @Test
    void fetchAll_mergesPages_andSortsById() {
        Extractor<Map<String,Object>> extractor =
                new FakeExtractor("fixtures/users_page1.json", "fixtures/users_page2.json");

        List<Map<String,Object>> all = extractor.fetchAll();

        assertEquals(3, all.size());
        assertEquals(1, ((Number)all.get(0).get("id")).intValue());
        assertEquals(2, ((Number)all.get(1).get("id")).intValue());
        assertEquals(3, ((Number)all.get(2).get("id")).intValue());
    }
}
