package org.hiast.batch.application.pipeline.filters;

import org.hiast.batch.application.pipeline.AnalyticsPipelineContext;
import org.hiast.batch.application.pipeline.Filter;
import org.hiast.batch.application.port.out.AnalyticsPersistencePort;
import org.hiast.batch.application.service.analytics.*;
import org.hiast.model.DataAnalytics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.List;

/**
 * Filter for processing medium-intensity analytics with moderate memory usage.
 * These analytics are processed with memory cleanup between each collector.
 */
public class MediumAnalyticsFilter implements Filter<AnalyticsPipelineContext, AnalyticsPipelineContext> {
    
    private static final Logger log = LoggerFactory.getLogger(MediumAnalyticsFilter.class);
    
    private final AnalyticsPersistencePort analyticsPersistence;

    public MediumAnalyticsFilter(AnalyticsPersistencePort analyticsPersistence) {
        this.analyticsPersistence = analyticsPersistence;
    }

    @Override
    public AnalyticsPipelineContext process(AnalyticsPipelineContext context) {
        log.info("Starting medium analytics processing...");
        
        if (!context.isLightweightAnalyticsCompleted()) {
            log.warn("Lightweight analytics not completed, skipping medium analytics");
            return context;
        }

        try {
            // Create medium analytics collectors
            List<AnalyticsCollector> mediumCollectors = Arrays.asList(
                new RatingDistributionAnalyticsService(),
                new UserAnalyticsService(),
                new TemporalTrendsAnalyticsService()
            );

            log.info("Processing {} medium analytics collectors", mediumCollectors.size());
            
            // Process each medium collector with memory cleanup
            for (int i = 0; i < mediumCollectors.size(); i++) {
                AnalyticsCollector collector = mediumCollectors.get(i);
                
                logMemoryUsage("Before " + collector.getAnalyticsType());
                processCollector(collector, context);
                
                // Memory cleanup between collectors
                if (i < mediumCollectors.size() - 1) {
                    performMemoryCleanup();
                }
                
                logMemoryUsage("After " + collector.getAnalyticsType());
            }

            context.setMediumAnalyticsCompleted(true);
            log.info("Medium analytics processing completed successfully");
            
        } catch (Exception e) {
            log.error("Error during medium analytics processing: {}", e.getMessage(), e);
            context.incrementFailedAnalytics();
        }

        return context;
    }

    private void processCollector(AnalyticsCollector collector, AnalyticsPipelineContext context) {
        String collectorType = collector.getAnalyticsType();
        
        try {
            log.info("Processing medium analytics: {}", collectorType);
            
            // Check if collector can process the data
            if (!collector.canProcess(context.getRatingsDf(), context.getRawMovies(), context.getRawTags())) {
                log.warn("Collector {} cannot process the available data, skipping", collectorType);
                return;
            }

            // Collect analytics
            List<DataAnalytics> analytics = collector.collectAnalytics(
                context.getRatingsDf(),
                context.getRawMovies(),
                context.getRawTags()
            );

            // Persist analytics
            for (DataAnalytics analytic : analytics) {
                boolean saved = analyticsPersistence.saveDataAnalytics(analytic);
                if (saved) {
                    context.addAnalytics(analytic);
                    log.info("✓ {} analytics saved successfully with ID: {}", 
                            collectorType, analytic.getAnalyticsId());
                } else {
                    context.incrementFailedAnalytics();
                    log.error("✗ Failed to save {} analytics with ID: {}", 
                            collectorType, analytic.getAnalyticsId());
                }
            }
            
        } catch (Exception e) {
            log.error("Error processing medium analytics {}: {}", collectorType, e.getMessage(), e);
            context.incrementFailedAnalytics();
        }
    }

    private void performMemoryCleanup() {
        log.info("Performing memory cleanup between medium analytics...");
        // System.gc(); // Explicit GC calls are generally not recommended
        // try {
        //     Thread.sleep(1000); // Brief pause for GC
        // } catch (InterruptedException e) {
        //     Thread.currentThread().interrupt();
        // }
        log.info("Memory cleanup step completed (explicit GC removed).");
    }

    private void logMemoryUsage(String operation) {
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;
        
        log.info("Memory {} - Used: {}MB, Free: {}MB, Total: {}MB", 
                operation,
                usedMemory / (1024 * 1024),
                freeMemory / (1024 * 1024),
                totalMemory / (1024 * 1024));
    }
}
