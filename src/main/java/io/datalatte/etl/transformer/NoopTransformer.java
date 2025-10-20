package io.datalatte.etl.transformer;

/**
 * No-op transformer: returns the input as-is.
 * Useful for wiring and baseline tests before real mapping exists.
 */
public class NoopTransformer<T> implements Transformer<T, T> {
    @Override
    public T apply(T in) {
        return in; // pass-through
    }
}
