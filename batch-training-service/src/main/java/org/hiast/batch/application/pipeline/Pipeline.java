package org.hiast.batch.application.pipeline;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * A pipeline that connects multiple filters.
 * Data flows through the pipeline, being processed by each filter in a sequence.
 *
 * @param <I> The input type of the first filter
 * @param <O> The output type of the last filter
 */
public class Pipeline<I, O> {
    private static final Logger log = LoggerFactory.getLogger(Pipeline.class);
    
    private final List<Filter<?, ?>> filters = new ArrayList<>();
    private final Class<O> outputType;

    /**
     * Creates a new pipeline with the specified output type.
     *
     * @param outputType The class of the output type
     */
    public Pipeline(Class<O> outputType) {
        this.outputType = outputType;
    }

    /**
     * Adds a filter to the pipeline.
     *
     * @param filter The filter to add
     * @param <T> The input type of the filter
     * @param <R> The output type of the filter
     * @return This pipeline for method chaining
     */
    public <T, R> Pipeline<I, O> addFilter(Filter<T, R> filter) {
        filters.add(filter);
        return this;
    }

    /**
     * Executes the pipeline with the given input.
     *
     * @param input The input to the pipeline
     * @return The output from the pipeline
     */
    @SuppressWarnings("unchecked")
    public O execute(I input) {
        if (filters.isEmpty()) {
            throw new IllegalStateException("Pipeline has no filters");
        }

        log.info("Starting pipeline execution with {} filters", filters.size());
        
        Object current = input;
        
        for (int i = 0; i < filters.size(); i++) {
            Filter<Object, Object> filter = (Filter<Object, Object>) filters.get(i);
            log.info("Executing filter {}: {}", i + 1, filter.getClass().getSimpleName());
            current = filter.process(current);
            
            if (current == null) {
                log.error("Filter {} returned null output", i + 1);
                throw new RuntimeException("Filter " + (i + 1) + " returned null output");
            }
        }
        
        if (!outputType.isInstance(current)) {
            throw new ClassCastException("Pipeline output type mismatch: expected " + 
                outputType.getName() + " but got " + current.getClass().getName());
        }
        
        log.info("Pipeline execution completed successfully");
        return outputType.cast(current);
    }
}