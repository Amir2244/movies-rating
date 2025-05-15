package org.hiast.batch.application.pipeline.filters;

import org.hiast.batch.application.pipeline.Filter;
import org.hiast.batch.application.pipeline.TrainingPipelineContext;
import org.hiast.batch.application.port.out.RatingDataProviderPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filter that loads raw ratings data from the data provider.
 */
public class DataLoadingFilter implements Filter<TrainingPipelineContext, TrainingPipelineContext> {
    private static final Logger log = LoggerFactory.getLogger(DataLoadingFilter.class);
    
    private final RatingDataProviderPort ratingDataProvider;
    
    public DataLoadingFilter(RatingDataProviderPort ratingDataProvider) {
        this.ratingDataProvider = ratingDataProvider;
    }
    
    @Override
    public TrainingPipelineContext process(TrainingPipelineContext context) {
        log.info("Loading raw ratings...");
        
        context.setRawRatings(ratingDataProvider.loadRawRatings(context.getSpark()));
        
        if (context.getRawRatings().isEmpty()) {
            log.error("No raw ratings data loaded. Aborting training.");
            throw new RuntimeException("No raw ratings data loaded");
        }
        
        log.info("Raw ratings loaded successfully");
        return context;
    }
}