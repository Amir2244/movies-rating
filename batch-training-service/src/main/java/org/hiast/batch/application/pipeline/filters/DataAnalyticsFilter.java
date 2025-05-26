package org.hiast.batch.application.pipeline.filters;

import org.hiast.batch.application.pipeline.ALSTrainingPipelineContext;
import org.hiast.batch.application.pipeline.Filter;
import org.hiast.batch.application.port.out.AnalyticsPersistencePort;
import org.hiast.batch.application.service.analytics.AnalyticsExecutionResult;
import org.hiast.batch.application.service.analytics.AnalyticsOrchestrator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Refactored filter that orchestrates analytics collection using the Strategy pattern.
 * 
 * This filter now follows the Single Responsibility Principle by delegating
 * analytics collection to specialized services through the AnalyticsOrchestrator.
 * 
 * The filter maintains its role in the pipeline while the actual analytics
 * collection is handled by domain services, preserving the Hexagonal Architecture.
 */
public class DataAnalyticsFilter implements Filter<ALSTrainingPipelineContext, ALSTrainingPipelineContext> {
    private static final Logger log = LoggerFactory.getLogger(DataAnalyticsFilter.class);

    private final AnalyticsOrchestrator analyticsOrchestrator;

    /**
     * Creates a new DataAnalyticsFilter with the analytics persistence port.
     * 
     * @param analyticsPersistence The port for persisting analytics data
     */
    public DataAnalyticsFilter(AnalyticsPersistencePort analyticsPersistence) {
        this.analyticsOrchestrator = new AnalyticsOrchestrator(analyticsPersistence);
        log.info("DataAnalyticsFilter initialized with {} analytics collectors", 
                analyticsOrchestrator.getCollectorCount());
    }

    @Override
    public ALSTrainingPipelineContext process(ALSTrainingPipelineContext context) {
        log.info("Starting analytics collection using orchestrator...");
        
        try {
            // Execute analytics collection through the orchestrator
            AnalyticsExecutionResult result = analyticsOrchestrator.executeAnalytics(context);
            
            // Log detailed results
            logAnalyticsResults(result);
            
            // Log summary
            if (result.hasFailures()) {
                log.warn("Analytics collection completed with some failures. " +
                        "Success: {}, Failed: {}, Skipped: {}, Total: {}",
                        result.getSuccessCount(), result.getFailureCount(), 
                        result.getSkippedCount(), result.getTotalCount());
            } else {
                log.info("Analytics collection completed successfully. " +
                        "Success: {}, Skipped: {}, Total: {}",
                        result.getSuccessCount(), result.getSkippedCount(), result.getTotalCount());
            }
            
        } catch (Exception e) {
            log.error("Critical error during analytics orchestration: {}", e.getMessage(), e);
        }
        
        return context;
    }
    
    /**
     * Logs detailed analytics results for monitoring and debugging.
     */
    private void logAnalyticsResults(AnalyticsExecutionResult result) {
        // Log successful analytics
        result.getSuccessfulResults().forEach(analyticsResult -> 
            log.info("✓ {} analytics completed successfully with ID: {}", 
                    analyticsResult.getAnalyticsType(), analyticsResult.getAnalyticsId()));
        
        // Log failed analytics
        result.getFailedResults().forEach(analyticsResult -> 
            log.error("✗ {} analytics failed: {}", 
                    analyticsResult.getAnalyticsType(), analyticsResult.getMessage()));
        
        // Log skipped analytics
        result.getResults().stream()
                .filter(r -> r.getStatus() == AnalyticsExecutionResult.AnalyticsStatus.SKIPPED)
                .forEach(analyticsResult -> 
                    log.warn("⚠ {} analytics skipped: {}", 
                            analyticsResult.getAnalyticsType(), analyticsResult.getMessage()));
    }
}
