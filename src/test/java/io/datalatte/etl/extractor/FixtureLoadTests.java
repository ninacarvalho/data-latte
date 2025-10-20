package io.datalatte.etl.extractor;

import io.datalatte.etl.TestFixtures;
import org.junit.jupiter.api.Test;
import java.util.List;
import java.util.Map;
import static org.assertj.core.api.Assertions.assertThat;

class FixtureLoadTests {
    @Test
    void canLoadUsersPages() throws Exception {
        List<Map<String,Object>> p1 = TestFixtures.loadListOfMaps("fixtures/users_page1.json");
        List<Map<String,Object>> p2 = TestFixtures.loadListOfMaps("fixtures/users_page2.json");
        assertThat(p1).hasSize(2);
        assertThat(p2).hasSize(1);
        assertThat(p1.get(0)).containsKeys("id","first_name","last_name","email","active");
    }
}
