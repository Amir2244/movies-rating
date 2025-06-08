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
 * Filter for processing lightweight analytics that have minimal memory impact.
 * These analytics can run together without overwhelming the cluster.
 */
public class LightweightAnalyticsFilter implements Filter<AnalyticsPipelineContext, AnalyticsPipelineContext> {
    
    private static final Logger log = LoggerFactory.getLogger(LightweightAnalyticsFilter.class);
    
    private final AnalyticsPersistencePort analyticsPersistence;

    public LightweightAnalyticsFilter(AnalyticsPersistencePort analyticsPersistence) {
        this.analyticsPersistence = analyticsPersistence;
    }

    @Override
    public AnalyticsPipelineContext process(AnalyticsPipelineContext context) {
        log.info("Starting lightweight analytics processing...");
        
        if (!context.isDataPreprocessed()) {
            log.warn("Data not preprocessed, skipping lightweight analytics");
            return context;
        }

        try {
            // Create lightweight analytics collectors
            List<AnalyticsCollector> lightweightCollectors = Arrays.asList(
                new DataQualityAnalyticsService(),
                new SystemPerformanceAnalyticsService(),
                new DataFreshnessAnalyticsService()
            );

            log.info("Processing {} lightweight analytics collectors", lightweightCollectors.size());
            
            // Process each lightweight collector
            for (AnalyticsCollector collector : lightweightCollectors) {
                processCollector(collector, context);
            }

            context.setLightweightAnalyticsCompleted(true);
            log.info("Lightweight analytics processing completed successfully");
            
        } catch (Exception e) {
            log.error("Error during lightweight analytics processing: {}", e.getMessage(), e);
            context.incrementFailedAnalytics();
        }

        return context;
    }

    private void processCollector(AnalyticsCollector collector, AnalyticsPipelineContext context) {
        String collectorType = collector.getAnalyticsType();
        
        try {
            log.info("Processing lightweight analytics: {}", collectorType);
            
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
            log.error("Error processing lightweight analytics {}: {}", collectorType, e.getMessage(), e);
            context.incrementFailedAnalytics();
        }
    }
}
