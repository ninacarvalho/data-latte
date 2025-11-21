package io.datalatte.etl.extractor;

public interface Extractor<T> {
    Iterable<T> fetchAll();
}
