package org.hiast.batch.util.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hiast.batch.application.pipeline.ALSTrainingPipelineContext;
import org.hiast.batch.application.port.out.AnalyticsPersistencePort;
import org.hiast.batch.domain.exception.AnalyticsCollectionException;
import org.hiast.batch.domain.model.DataAnalytics;
import org.hiast.batch.application.service.factory.AnalyticsCollectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

/**
 * Orchestrator for analytics collection and persistence.
 * 
 * This class coordinates the execution of multiple analytics collectors
 * while maintaining the Hexagonal Architecture by depending only on
 * domain services and application ports.
 * 
 * Responsibilities:
 * - Coordinate analytics collection across multiple collectors
 * - Handle errors gracefully without stopping the entire process
 * - Persist analytics results through the persistence port
 * - Provide comprehensive logging and monitoring
 */
public class AnalyticsOrchestrator {
    
    private static final Logger log = LoggerFactory.getLogger(AnalyticsOrchestrator.class);
    
    private final AnalyticsPersistencePort analyticsPersistence;
    private final List<AnalyticsCollector> collectors;
    private final ExecutorService executorService;
    
    /**
     * Creates a new analytics orchestrator with default collectors.
     * 
     * @param analyticsPersistence The persistence port for saving analytics
     */
    public AnalyticsOrchestrator(AnalyticsPersistencePort analyticsPersistence) {
        this.analyticsPersistence = analyticsPersistence;
        this.collectors = AnalyticsCollectorFactory.createAllCollectors();
        this.executorService = Executors.newFixedThreadPool(Math.min(collectors.size(), 4));
        
        log.info("Analytics orchestrator initialized with {} collectors", collectors.size());
    }
    
    /**
     * Creates a new analytics orchestrator with custom collectors.
     * 
     * @param analyticsPersistence The persistence port for saving analytics
     * @param collectors The list of analytics collectors to use
     */
    public AnalyticsOrchestrator(AnalyticsPersistencePort analyticsPersistence, 
                               List<AnalyticsCollector> collectors) {
        this.analyticsPersistence = analyticsPersistence;
        this.collectors = new ArrayList<>(collectors);
        this.executorService = Executors.newFixedThreadPool(Math.min(collectors.size(), 4));
        
        log.info("Analytics orchestrator initialized with {} custom collectors", collectors.size());
    }
    
    /**
     * Executes all analytics collection and persistence.
     * 
     * @param context The pipeline context containing all datasets
     * @return AnalyticsExecutionResult containing success/failure information
     */
    public AnalyticsExecutionResult executeAnalytics(ALSTrainingPipelineContext context) {
        log.info("Starting comprehensive analytics collection...");
        
        Dataset<Row> ratingsDf = context.getRatingsDf();
        Dataset<Row> moviesData = context.getRawMovies();
        Dataset<Row> tagsData = context.getRawTags();
        
        if (ratingsDf == null || ratingsDf.isEmpty()) {
            log.warn("Ratings data is empty or null, skipping analytics collection.");
            return AnalyticsExecutionResult.empty("No ratings data available");
        }
        
        // Register temporary views for easier SQL access
        registerTemporaryViews(moviesData, tagsData);
        
        AnalyticsExecutionResult.Builder resultBuilder = AnalyticsExecutionResult.builder();
        
        // Execute analytics collection for each collector
        for (AnalyticsCollector collector : collectors) {
            executeCollectorAnalytics(collector, ratingsDf, moviesData, tagsData, context, resultBuilder);
        }
        
        AnalyticsExecutionResult result = resultBuilder.build();
        
        log.info("Analytics collection completed. Success: {}, Failed: {}, Total: {}", 
                result.getSuccessCount(), result.getFailureCount(), result.getTotalCount());
        
        return result;
    }
    
    /**
     * Executes analytics collection asynchronously for better performance.
     * 
     * @param context The pipeline context containing all datasets
     * @return CompletableFuture with the execution result
     */
    public CompletableFuture<AnalyticsExecutionResult> executeAnalyticsAsync(ALSTrainingPipelineContext context) {
        return CompletableFuture.supplyAsync(() -> executeAnalytics(context), executorService);
    }
    
    /**
     * Executes analytics collection for a specific collector.
     */
    private void executeCollectorAnalytics(AnalyticsCollector collector,
                                         Dataset<Row> ratingsDf,
                                         Dataset<Row> moviesData,
                                         Dataset<Row> tagsData,
                                         ALSTrainingPipelineContext context,
                                         AnalyticsExecutionResult.Builder resultBuilder) {
        
        String collectorType = collector.getAnalyticsType();
        
        try {
            log.info("Executing analytics collector: {}", collectorType);

            // Check if collector can process the data
            if (!collector.canProcess(ratingsDf, moviesData, tagsData)) {
                log.warn("Collector {} cannot process the available data, skipping", collectorType);
                resultBuilder.addSkipped(collectorType, "Insufficient data for processing");
                return;
            }

            // Collect analytics
            List<DataAnalytics> analytics = collector.collectAnalytics(ratingsDf, moviesData, tagsData, context);

           for( DataAnalytics analytic : analytics ) {
               boolean saved = persistAnalytics(analytic);

               if (saved) {
                   log.info("✓ {} analytics completed and saved successfully with ID: {}",
                           collectorType, analytic.getAnalyticsId());
                   resultBuilder.addSuccess(collectorType, analytic.getAnalyticsId());
               } else {
                   log.error("✗ Failed to save {} analytics with ID: {}", collectorType, analytic.getAnalyticsId());
                   resultBuilder.addFailure(collectorType, "Failed to persist analytics", null);
               }
           }
        } catch (AnalyticsCollectionException e) {
            log.error("✗ Analytics collection failed for {}: {}", collectorType, e.getMessage());
            resultBuilder.addFailure(collectorType, e.getMessage(), e);

        } catch (Exception e) {
            log.error("✗ Unexpected error in {} analytics: {}", collectorType, e.getMessage(), e);
            resultBuilder.addFailure(collectorType, "Unexpected error: " + e.getMessage(), e);
        }

    }
    
    /**
     * Persists analytics data through the persistence port.
     */
    private boolean persistAnalytics(DataAnalytics analytics) {
        try {
            return analyticsPersistence.saveDataAnalytics(analytics);
        } catch (Exception e) {
            log.error("Error persisting analytics {}: {}", analytics.getAnalyticsId(), e.getMessage(), e);
            return false;
        }
    }
    
    /**
     * Registers temporary views for easier SQL access.
     */
    private void registerTemporaryViews(Dataset<Row> moviesData, Dataset<Row> tagsData) {
        try {
            if (moviesData != null && !moviesData.isEmpty()) {
                moviesData.createOrReplaceTempView("movies");
                log.debug("Registered movies data as temporary view 'movies'");
            } else {
                log.warn("Movies data is empty or null, genre analytics will be limited");
            }
            
            if (tagsData != null && !tagsData.isEmpty()) {
                tagsData.createOrReplaceTempView("tags");
                log.debug("Registered tags data as temporary view 'tags'");
            } else {
                log.warn("Tags data is empty or null, tag-based analytics will be unavailable");
            }
        } catch (Exception e) {
            log.warn("Error registering temporary views: {}", e.getMessage());
        }
    }
    
    /**
     * Shuts down the executor service.
     */
    public void shutdown() {
        if (executorService != null && !executorService.isShutdown()) {
            executorService.shutdown();
            log.info("Analytics orchestrator executor service shut down");
        }
    }
    
    /**
     * Gets the number of configured collectors.
     * 
     * @return The number of collectors
     */
    public int getCollectorCount() {
        return collectors.size();
    }
    
    /**
     * Gets the list of collector types.
     * 
     * @return List of analytics types handled by the collectors
     */
    public List<String> getCollectorTypes() {
        return collectors.stream()
                .map(AnalyticsCollector::getAnalyticsType)
                .collect(Collectors.toList());
    }
}
