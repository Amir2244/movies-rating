package org.hiast.batch.application.service.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hiast.batch.application.pipeline.ALSTrainingPipelineContext;
import org.hiast.batch.domain.exception.AnalyticsCollectionException;
import org.hiast.batch.domain.model.AnalyticsType;
import org.hiast.batch.domain.model.DataAnalytics;
import org.hiast.batch.domain.model.analytics.AnalyticsCollector;
import org.hiast.batch.domain.model.analytics.AnalyticsMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.UUID;

/**
 * Service responsible for collecting system performance analytics.
 * Monitors batch processing performance, efficiency, and resource utilization
 * to ensure optimal system operation in production environments.
 * 
 * This service provides insights into processing efficiency, data quality
 * impact on performance, and system resource utilization patterns.
 */
public class SystemPerformanceAnalyticsService implements AnalyticsCollector {
    
    private static final Logger log = LoggerFactory.getLogger(SystemPerformanceAnalyticsService.class);
    
    @Override
    public DataAnalytics collectAnalytics(Dataset<Row> ratingsDf, 
                                        Dataset<Row> moviesData, 
                                        Dataset<Row> tagsData, 
                                        ALSTrainingPipelineContext context) {
        
        if (!canProcess(ratingsDf, moviesData, tagsData)) {
            throw new AnalyticsCollectionException("SYSTEM_PERFORMANCE", "Insufficient data for system performance analytics");
        }
        
        try {
            log.info("Collecting processing performance analytics...");
            
            AnalyticsMetrics metrics = AnalyticsMetrics.builder();
            
            // Collect processing performance metrics - exact same as original
            collectProcessingPerformanceMetrics(ratingsDf, context, metrics);
            
            // Collect additional system metrics
            collectSystemResourceMetrics(ratingsDf, moviesData, tagsData, metrics);
            
            // Collect data quality impact metrics
            collectDataQualityImpactMetrics(ratingsDf, metrics);
            
            log.info("Processing performance analytics collection completed with {} metrics", metrics.size());
            
            return new DataAnalytics(
                    "processing_performance_" + UUID.randomUUID().toString().substring(0, 8),
                    Instant.now(),
                    AnalyticsType.PROCESSING_PERFORMANCE,
                    metrics.build(),
                    "Batch processing performance and efficiency analytics"
            );
            
        } catch (Exception e) {
            log.error("Error collecting processing performance analytics: {}", e.getMessage(), e);
            throw new AnalyticsCollectionException("SYSTEM_PERFORMANCE", e.getMessage(), e);
        }
    }
    
    /**
     * Collects processing performance metrics - exact same as original implementation.
     */
    private void collectProcessingPerformanceMetrics(Dataset<Row> ratingsDf, ALSTrainingPipelineContext context, 
                                                   AnalyticsMetrics metrics) {
        log.debug("Collecting processing performance metrics...");
        
        long startTime = System.currentTimeMillis();

        // Basic dataset metrics - exact same as original
        long totalRecords = ratingsDf.count();
        int partitionCount = ratingsDf.rdd().getNumPartitions();
        
        // Calculate processing metrics - exact same as original
        long processingTime = System.currentTimeMillis() - startTime;
        double recordsPerSecond = totalRecords / (processingTime / 1000.0);

        // Memory and storage metrics (approximations) - exact same as original
        long estimatedMemoryUsage = ratingsDf.storageLevel().useMemory() ? 
                totalRecords * 32 : 0; // Rough estimate: 32 bytes per record

        // Add metrics - exact same keys as original
        metrics.addMetric("totalRecords", totalRecords)
               .addMetric("partitionCount", partitionCount)
               .addMetric("processingTimeMs", processingTime)
               .addMetric("recordsPerSecond", recordsPerSecond)
               .addMetric("estimatedMemoryUsageBytes", estimatedMemoryUsage)
               .addMetric("recordsPerPartition", totalRecords / partitionCount);

        // Data quality indicators that affect performance - exact same as original
        long nullRecords = ratingsDf.filter(
                functions.col("userId").isNull()
                        .or(functions.col("movieId").isNull())
                        .or(functions.col("ratingActual").isNull())
        ).count();

        metrics.addMetric("nullRecords", nullRecords)
               .addMetric("dataQualityScore", (double) (totalRecords - nullRecords) / totalRecords * 100);

        // Processing efficiency metrics - exact same as original
        metrics.addMetric("processingEfficiency", recordsPerSecond > 1000 ? "High" : 
                recordsPerSecond > 100 ? "Medium" : "Low");
        
        log.debug("Processing performance metrics collected");
    }
    
    /**
     * Collects system resource metrics.
     */
    private void collectSystemResourceMetrics(Dataset<Row> ratingsDf, Dataset<Row> moviesData, 
                                            Dataset<Row> tagsData, AnalyticsMetrics metrics) {
        log.debug("Collecting system resource metrics...");
        
        // Calculate dataset sizes and resource usage
        long ratingsSize = ratingsDf.count();
        long moviesSize = moviesData != null && !moviesData.isEmpty() ? moviesData.count() : 0;
        long tagsSize = tagsData != null && !tagsData.isEmpty() ? tagsData.count() : 0;
        
        metrics.addMetric("ratingsDatasetSize", ratingsSize)
               .addMetric("moviesDatasetSize", moviesSize)
               .addMetric("tagsDatasetSize", tagsSize)
               .addMetric("totalDatasetSize", ratingsSize + moviesSize + tagsSize);
        
        // Calculate partition efficiency
        int ratingsPartitions = ratingsDf.rdd().getNumPartitions();
        double avgRecordsPerPartition = (double) ratingsSize / ratingsPartitions;
        
        metrics.addMetric("ratingsPartitions", ratingsPartitions)
               .addMetric("avgRecordsPerPartition", avgRecordsPerPartition);
        
        // Estimate resource utilization
        boolean isWellPartitioned = avgRecordsPerPartition > 1000 && avgRecordsPerPartition < 100000;
        metrics.addMetric("isWellPartitioned", isWellPartitioned);
        
        // Calculate data skew indicators
        if (ratingsSize > 0) {
            // Simple skew detection based on user distribution
            Dataset<Row> userCounts = ratingsDf.groupBy("userId").count();
            Row userStats = userCounts.agg(
                    functions.min("count").as("minUserRatings"),
                    functions.max("count").as("maxUserRatings"),
                    functions.avg("count").as("avgUserRatings")
            ).first();
            
            long minUserRatings = userStats.getLong(0);
            long maxUserRatings = userStats.getLong(1);
            double avgUserRatings = userStats.getDouble(2);
            
            double skewRatio = maxUserRatings / (double) Math.max(minUserRatings, 1);
            
            metrics.addMetric("minUserRatings", minUserRatings)
                   .addMetric("maxUserRatings", maxUserRatings)
                   .addMetric("avgUserRatings", avgUserRatings)
                   .addMetric("userDataSkewRatio", skewRatio)
                   .addMetric("hasSignificantSkew", skewRatio > 100);
        }
        
        log.debug("System resource metrics collected");
    }
    
    /**
     * Collects data quality impact on performance metrics.
     */
    private void collectDataQualityImpactMetrics(Dataset<Row> ratingsDf, AnalyticsMetrics metrics) {
        log.debug("Collecting data quality impact metrics...");
        
        long startTime = System.currentTimeMillis();
        
        // Measure time for data quality checks
        long totalRows = ratingsDf.count();
        long validRows = ratingsDf.filter(
                functions.col("userId").isNotNull()
                .and(functions.col("movieId").isNotNull())
                .and(functions.col("ratingActual").isNotNull())
                .and(functions.col("timestampEpochSeconds").isNotNull())
        ).count();
        
        long qualityCheckTime = System.currentTimeMillis() - startTime;
        
        // Calculate quality impact metrics
        double dataQualityRatio = (double) validRows / totalRows;
        double qualityImpactOnPerformance = (1.0 - dataQualityRatio) * 100;
        
        metrics.addMetric("qualityCheckTimeMs", qualityCheckTime)
               .addMetric("validRowsCount", validRows)
               .addMetric("invalidRowsCount", totalRows - validRows)
               .addMetric("dataQualityRatio", dataQualityRatio)
               .addMetric("qualityImpactOnPerformance", qualityImpactOnPerformance);
        
        // Performance recommendations based on data quality
        String performanceRecommendation;
        if (dataQualityRatio > 0.95) {
            performanceRecommendation = "Excellent data quality - optimal performance expected";
        } else if (dataQualityRatio > 0.90) {
            performanceRecommendation = "Good data quality - minor performance impact";
        } else if (dataQualityRatio > 0.80) {
            performanceRecommendation = "Fair data quality - moderate performance impact";
        } else {
            performanceRecommendation = "Poor data quality - significant performance impact";
        }
        
        metrics.addMetric("performanceRecommendation", performanceRecommendation);
        
        // Calculate processing efficiency score
        double efficiencyScore = dataQualityRatio * 100;
        if (qualityCheckTime < 1000) efficiencyScore += 10; // Bonus for fast quality checks
        if (qualityCheckTime > 5000) efficiencyScore -= 10; // Penalty for slow quality checks
        
        metrics.addMetric("processingEfficiencyScore", Math.min(100, Math.max(0, efficiencyScore)));
        
        log.debug("Data quality impact metrics collected");
    }
    
    @Override
    public String getAnalyticsType() {
        return "SYSTEM_PERFORMANCE";
    }
    
    @Override
    public boolean canProcess(Dataset<Row> ratingsDf, Dataset<Row> moviesData, Dataset<Row> tagsData) {
        return ratingsDf != null && !ratingsDf.isEmpty();
    }
    
    @Override
    public int getPriority() {
        return 60; // Lower priority for system performance monitoring
    }
}
