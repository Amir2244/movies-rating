package org.hiast.batch.application.pipeline.filters;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hiast.batch.application.pipeline.Filter;
import org.hiast.batch.application.pipeline.BasePipelineContext;
import org.hiast.batch.application.port.out.RatingDataProviderPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filter that loads additional MovieLens data files (movies, tags, links) from HDFS.
 * This filter runs after the main ratings data is loaded and provides additional
 * metadata for comprehensive analytics.
 * Works with both ALSTrainingPipelineContext and AnalyticsPipelineContext via BasePipelineContext.
 */
public class MovieDataLoadingFilter<T extends BasePipelineContext> implements Filter<T, T> {
    private static final Logger log = LoggerFactory.getLogger(MovieDataLoadingFilter.class);
    
    private final RatingDataProviderPort ratingDataProvider;
    
    public MovieDataLoadingFilter(RatingDataProviderPort ratingDataProvider) {
        this.ratingDataProvider = ratingDataProvider;
    }
    
    @Override
    public T process(T context) {
        log.info("Loading additional MovieLens data files...");
        
        try {
            // Load movies metadata
            log.info("Loading movies metadata...");
            Dataset<Row> rawMovies = ratingDataProvider.loadRawMovies(context.getSpark());
            context.setRawMovies(rawMovies);
            
            if (rawMovies != null && !rawMovies.isEmpty()) {
                long movieCount = rawMovies.count();
                log.info("Successfully loaded {} movies with metadata", movieCount);
            } else {
                log.warn("Movies data is empty or null - genre analytics will be limited");
            }
            
            // Load tags data
            log.info("Loading tags data...");
            Dataset<Row> rawTags = ratingDataProvider.loadRawTags(context.getSpark());
            context.setRawTags(rawTags);
            
            if (rawTags != null && !rawTags.isEmpty()) {
                long tagCount = rawTags.count();
                log.info("Successfully loaded {} tag records", tagCount);
            } else {
                log.warn("Tags data is empty or null - tag-based analytics will be unavailable");
            }
            
            // Load links data
            log.info("Loading links data...");
            Dataset<Row> rawLinks = ratingDataProvider.loadRawLinks(context.getSpark());
            context.setRawLinks(rawLinks);
            
            if (rawLinks != null && !rawLinks.isEmpty()) {
                long linkCount = rawLinks.count();
                log.info("Successfully loaded {} link records", linkCount);
            } else {
                log.warn("Links data is empty or null - external ID linking will be unavailable");
            }
            
            log.info("Additional MovieLens data loading completed successfully");
            
        } catch (Exception e) {
            log.error("Error loading additional MovieLens data: {}", e.getMessage(), e);
            // Don't fail the pipeline - set empty datasets and continue
            if (context.getRawMovies() == null) {
                context.setRawMovies(context.getSpark().emptyDataFrame());
            }
            if (context.getRawTags() == null) {
                context.setRawTags(context.getSpark().emptyDataFrame());
            }
            if (context.getRawLinks() == null) {
                context.setRawLinks(context.getSpark().emptyDataFrame());
            }
            log.warn("Continuing pipeline with empty additional datasets due to loading errors");
        }

        context.setMovieDataLoaded(true);
        return context;
    }
}
