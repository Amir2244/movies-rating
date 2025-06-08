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
 * Filter for processing heavy analytics that have high memory usage.
 * These analytics are processed one at a time with extensive memory cleanup.
 */
public class HeavyAnalyticsFilter implements Filter<AnalyticsPipelineContext, AnalyticsPipelineContext> {
    
    private static final Logger log = LoggerFactory.getLogger(HeavyAnalyticsFilter.class);
    
    private final AnalyticsPersistencePort analyticsPersistence;

    public HeavyAnalyticsFilter(AnalyticsPersistencePort analyticsPersistence) {
        this.analyticsPersistence = analyticsPersistence;
    }

    @Override
    public AnalyticsPipelineContext process(AnalyticsPipelineContext context) {
        log.info("Starting heavy analytics processing...");
        
        if (!context.isMediumAnalyticsCompleted()) {
            log.warn("Medium analytics not completed, skipping heavy analytics");
            return context;
        }

        try {
            // Create heavy analytics collectors - these are memory intensive
            List<AnalyticsCollector> heavyCollectors = Arrays.asList(
                // Uncomment these one by one as needed based on memory availability
                 new ContentAnalyticsService(),
                 new TagAnalyticsService(),
                 new UserSegmentationAnalyticsService(),
                 new UserEngagementAnalyticsService()
            );

            if (heavyCollectors.isEmpty()) {
                log.info("No heavy analytics collectors enabled, skipping heavy analytics processing");
                context.setHeavyAnalyticsCompleted(true);
                return context;
            }

            log.info("Processing {} heavy analytics collectors (one at a time)", heavyCollectors.size());
            
            // Process each heavy collector individually with extensive cleanup
            for (int i = 0; i < heavyCollectors.size(); i++) {
                AnalyticsCollector collector = heavyCollectors.get(i);
                
                log.info("Starting heavy analytics collector {}/{}: {}", 
                        i + 1, heavyCollectors.size(), collector.getAnalyticsType());
                
                logMemoryUsage("Before " + collector.getAnalyticsType());
                processHeavyCollector(collector, context);
                
                // Extensive memory cleanup after each heavy collector
                performExtensiveMemoryCleanup();
                logMemoryUsage("After " + collector.getAnalyticsType() + " cleanup");
            }

            context.setHeavyAnalyticsCompleted(true);
            log.info("Heavy analytics processing completed successfully");
            
        } catch (Exception e) {
            log.error("Error during heavy analytics processing: {}", e.getMessage(), e);
            context.incrementFailedAnalytics();
        }

        return context;
    }

    private void processHeavyCollector(AnalyticsCollector collector, AnalyticsPipelineContext context) {
        String collectorType = collector.getAnalyticsType();
        
        try {
            log.info("Processing heavy analytics: {}", collectorType);
            
            // Check if collector can process the data
            if (!collector.canProcess(context.getRatingsDf(), context.getRawMovies(), context.getRawTags())) {
                log.warn("Collector {} cannot process the available data, skipping", collectorType);
                return;
            }

            // Collect analytics with memory monitoring
            long startTime = System.currentTimeMillis();
            List<DataAnalytics> analytics = collector.collectAnalytics(
                context.getRatingsDf(),
                context.getRawMovies(),
                context.getRawTags()
            );
            long endTime = System.currentTimeMillis();
            
            log.info("Heavy analytics {} completed in {} ms", collectorType, endTime - startTime);

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
            log.error("Error processing heavy analytics {}: {}", collectorType, e.getMessage(), e);
            context.incrementFailedAnalytics();
        }
    }

    private void performExtensiveMemoryCleanup() {
        log.info("Performing extensive memory cleanup after heavy analytics...");
        
        // Multiple GC calls for heavy cleanup
        // for (int i = 0; i < 3; i++) {
        //     System.gc();
        //     try {
        //         Thread.sleep(1000); // Longer pause for extensive cleanup
        //     } catch (InterruptedException e) {
        //         Thread.currentThread().interrupt();
        //         break;
        //     }
        // }
        
        log.info("Extensive memory cleanup step completed (explicit GCs removed).");
    }

    private void logMemoryUsage(String operation) {
        Runtime runtime = Runtime.getRuntime();
        long totalMemory = runtime.totalMemory();
        long freeMemory = runtime.freeMemory();
        long usedMemory = totalMemory - freeMemory;
        long maxMemory = runtime.maxMemory();
        
        log.info("Memory {} - Used: {}MB, Free: {}MB, Total: {}MB, Max: {}MB", 
                operation,
                usedMemory / (1024 * 1024),
                freeMemory / (1024 * 1024),
                totalMemory / (1024 * 1024),
                maxMemory / (1024 * 1024));
    }
}
