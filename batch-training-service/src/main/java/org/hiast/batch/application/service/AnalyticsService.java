package org.hiast.batch.application.service;

import org.apache.spark.sql.SparkSession;
import org.hiast.batch.application.pipeline.Pipeline;
import org.hiast.batch.application.pipeline.AnalyticsPipelineContext;
import org.hiast.batch.application.pipeline.filters.*;
import org.hiast.batch.application.port.in.AnalyticsUseCase;
import org.hiast.batch.application.port.out.AnalyticsPersistencePort;
import org.hiast.batch.application.port.out.RatingDataProviderPort;
import org.hiast.batch.config.HDFSConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Application service implementing the AnalyticsUseCase.
 * Orchestrates the analytics processing pipeline using injected ports for data access and persistence.
 * Follows the same architectural pattern as ALSModelTrainerService.
 */
public class AnalyticsService implements AnalyticsUseCase {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsService.class);

    private final SparkSession spark;
    private final RatingDataProviderPort ratingDataProvider;
    private final AnalyticsPersistencePort analyticsPersistence;
    private final HDFSConfig hdfsConfig;

    public AnalyticsService(SparkSession spark,
                           RatingDataProviderPort ratingDataProvider,
                           AnalyticsPersistencePort analyticsPersistence,
                           HDFSConfig hdfsConfig) {
        this.spark = spark;
        this.ratingDataProvider = ratingDataProvider;
        this.analyticsPersistence = analyticsPersistence;
        this.hdfsConfig = hdfsConfig;
    }

    @SuppressWarnings("unchecked") // Suppress warnings due to Pipeline's internal use of raw types for filters
    public void executeAnalyticsPipeline() {
        log.info("Starting analytics pipeline using pipes and filter pattern...");
        AnalyticsPipelineContext context = null; // Initialize to null

        try {
            // Create the pipeline context
            context = new AnalyticsPipelineContext(spark);

            // Create the pipeline with all filters
            Pipeline<AnalyticsPipelineContext, AnalyticsPipelineContext> pipeline = 
                new Pipeline<>(AnalyticsPipelineContext.class);

            // Reuse existing filters - they now work with BasePipelineContext
            pipeline.addFilter(new DataLoadingFilter<>(ratingDataProvider))
                    .addFilter(new MovieDataLoadingFilter<>(ratingDataProvider))
                    .addFilter(new DataPreprocessingFilter<>(ratingDataProvider))
                    .addFilter(new LightweightAnalyticsFilter(analyticsPersistence))
                    .addFilter(new MediumAnalyticsFilter(analyticsPersistence))
                    .addFilter(new HeavyAnalyticsFilter(analyticsPersistence));

            // Execute the pipeline
            AnalyticsPipelineContext result = pipeline.execute(context);

            // Mark analytics as completed and check the result
            result.setAnalyticsCompleted(true);

            if (result.getSuccessfulAnalyticsCount() > 0) {
                log.info("Analytics pipeline finished successfully. Successful: {}, Failed: {}, Total: {}",
                        result.getSuccessfulAnalyticsCount(), result.getFailedAnalyticsCount(),
                        result.getTotalAnalyticsCount());
            } else {
                log.warn("Analytics pipeline finished with no successful analytics. Failed: {}",
                        result.getFailedAnalyticsCount());
            }
        } catch (Exception e) {
            log.error("Error during analytics pipeline: {}", e.getMessage(), e);
            throw e;
        } finally {
            if (context != null) {
                log.info("Cleaning up analytics pipeline context data...");
                context.cleanupRawData();
                context.cleanupProcessedData();
                log.info("Analytics pipeline context data cleanup complete.");
            }
        }
    }
}
