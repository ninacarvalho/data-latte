package io.datalatte.etl.pipeline;

/**
 * Defines the contract for an ETL (Extract, Transform, Load) pipeline.
 * Implementations of this interface are responsible for executing a data processing workflow.
 */
public interface Pipeline {

    /**
     * Executes the pipeline a single time.
     */
    void runOnce();
}
