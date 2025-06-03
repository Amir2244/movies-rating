package org.hiast.batch.application.pipeline.filters;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hiast.batch.application.pipeline.Filter;
import org.hiast.batch.application.pipeline.BasePipelineContext;
import org.hiast.batch.application.port.out.RatingDataProviderPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filter that loads MovieLens metadata for analytics purposes.
 * Note: This filter is now primarily used in the analytics pipeline.
 * The training pipeline uses StreamingMovieMetaDataEnrichmentFilter which loads
 * movie data on-demand during the enrichment phase.
 */
public class MovieDataLoadingFilter<T extends BasePipelineContext> implements Filter<T, T> {
    private static final Logger log = LoggerFactory.getLogger(MovieDataLoadingFilter.class);
    
    private final RatingDataProviderPort ratingDataProvider;
    
    public MovieDataLoadingFilter(RatingDataProviderPort ratingDataProvider) {
        this.ratingDataProvider = ratingDataProvider;
    }
    
    @Override
    public T process(T context) {
        log.info("Loading movie metadata for analytics...");
        
        try {
            // Load movie data
            log.info("Loading movies metadata...");
            Dataset<Row> rawMovies = ratingDataProvider.loadRawMovies(context.getSpark());
            
            if (rawMovies != null && !rawMovies.isEmpty()) {
                // Select only essential columns and cache
                Dataset<Row> essentialMovies = rawMovies
                    .select("movieId", "title", "genres")
                    .cache();
                
                long movieCount = essentialMovies.count();
                log.info("Loaded {} movies with metadata", movieCount);
                
                context.setRawMovies(essentialMovies);
            } else {
                log.warn("Movies data is empty or null - genre analytics will be limited");
                context.setRawMovies(context.getSpark().emptyDataFrame());
            }
            
            // Load tags only if needed for analytics
            if (shouldLoadTags(context)) {
                log.info("Loading tags data...");
                Dataset<Row> rawTags = ratingDataProvider.loadRawTags(context.getSpark());
                
                if (rawTags != null && !rawTags.isEmpty()) {
                    // Only keep essential tag columns
                    Dataset<Row> essentialTags = rawTags
                        .select("movieId", "tag")
                        .cache();
                    
                    context.setRawTags(essentialTags);
                    log.info("Successfully loaded {} tag records", essentialTags.count());
                } else {
                    context.setRawTags(context.getSpark().emptyDataFrame());
                }
            } else {
                log.info("Skipping tags loading - not required for current pipeline");
                context.setRawTags(context.getSpark().emptyDataFrame());
            }
            
            // Skip links data unless explicitly needed
            context.setRawLinks(context.getSpark().emptyDataFrame());
            
            log.info("Movie metadata loading completed");
            
        } catch (Exception e) {
            log.error("Error loading movie metadata: {}", e.getMessage(), e);
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
        }

        context.setMovieDataLoaded(true);
        return context;
    }
    
    private boolean shouldLoadTags(T context) {
        // Load tags only for analytics pipeline
        String className = context.getClass().getSimpleName();
        return className.contains("Analytics");
    }
}
