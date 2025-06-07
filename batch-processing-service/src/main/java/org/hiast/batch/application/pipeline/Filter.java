package org.hiast.batch.application.pipeline;

/**
 * Interface for a filter in the pipes and filter pattern.
 * Each filter processes input data and produces output data.
 *
 * @param <I> The input type
 * @param <O> The output type
 */
public interface Filter<I, O> {
    /**
     * Process the input data and produce output data.
     *
     * @param input The input data
     * @return The processed output data
     */
    O process(I input);
}