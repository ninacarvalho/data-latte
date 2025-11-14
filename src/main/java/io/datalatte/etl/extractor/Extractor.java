package io.datalatte.etl.extractor;

import java.util.List;

public interface Extractor<T> {
    Iterable<T> fetchAll();
}
