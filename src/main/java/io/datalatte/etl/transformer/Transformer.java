package io.datalatte.etl.transformer;

public interface Transformer<I,O> {
    O apply(I in);
}