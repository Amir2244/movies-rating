package org.hiast.batch.application.pipeline.filters;

import org.hiast.batch.application.pipeline.Filter;
import org.hiast.batch.application.pipeline.BasePipelineContext;
import org.hiast.batch.application.pipeline.ALSTrainingPipelineContext;
import org.hiast.batch.application.port.out.RatingDataProviderPort;
import org.hiast.batch.domain.exception.DataLoadingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filter that loads raw ratings data from the data provider.
 * Works with both ALSTrainingPipelineContext and AnalyticsPipelineContext via BasePipelineContext.
 */
public class DataLoadingFilter<T extends BasePipelineContext> implements Filter<T, T> {
    private static final Logger log = LoggerFactory.getLogger(DataLoadingFilter.class);

    private final RatingDataProviderPort ratingDataProvider;

    public DataLoadingFilter(RatingDataProviderPort ratingDataProvider) {
        this.ratingDataProvider = ratingDataProvider;
    }

    @Override
    public T process(T context) {
        log.info("Loading raw ratings...");

        context.setRawRatings(ratingDataProvider.loadRawRatings(context.getSpark()));

        if (context.getRawRatings().isEmpty()) {
            log.error("No raw ratings data loaded. Aborting pipeline.");
            throw new DataLoadingException("No raw ratings data loaded. Please check the data source and configuration.");
        }

        context.setDataLoaded(true);
        log.info("Raw ratings loaded successfully");

        // Call training-specific method if it's a training context
        if (context instanceof ALSTrainingPipelineContext) {
            ((ALSTrainingPipelineContext) context).markDataLoadingCompleted();
        }

        return context;
    }
}