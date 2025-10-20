package io.datalatte.etl.pipeline;

/** Marker for pipeline orchestration (extract -> transform -> load). */
public interface Pipeline {
    void runOnce(); // TODO: wire extractor+transformer+producer
}