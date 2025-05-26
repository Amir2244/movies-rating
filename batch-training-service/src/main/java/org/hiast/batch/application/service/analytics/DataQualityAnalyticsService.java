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
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Service responsible for collecting data quality analytics.
 * Handles data completeness, freshness, and consistency analytics.
 * 
 * This service focuses on data quality metrics that are crucial
 * for monitoring the health and reliability of the dataset.
 */
public class DataQualityAnalyticsService implements AnalyticsCollector {
    
    private static final Logger log = LoggerFactory.getLogger(DataQualityAnalyticsService.class);
    
    @Override
    public List<DataAnalytics> collectAnalytics(Dataset<Row> ratingsDf,
                                                Dataset<Row> moviesData,
                                                Dataset<Row> tagsData,
                                                ALSTrainingPipelineContext context) {
        
        if (!canProcess(ratingsDf, moviesData, tagsData)) {
            throw new AnalyticsCollectionException("DATA_QUALITY", "Insufficient data for data quality analytics");
        }
        
        try {
            log.info("Collecting data quality analytics...");
            
            AnalyticsMetrics metrics = AnalyticsMetrics.builder();
            
            // Collect data completeness metrics
            collectDataCompletenessMetrics(ratingsDf, metrics);
            
            // Collect data freshness metrics
            collectDataFreshnessMetrics(ratingsDf, metrics);
            
            // Collect data consistency metrics
            collectDataConsistencyMetrics(ratingsDf, moviesData, tagsData, metrics);
            
            log.info("Data quality analytics collection completed with {} metrics", metrics.size());
            
            return Collections.singletonList(new DataAnalytics(
                    "data_quality_" + UUID.randomUUID().toString().substring(0, 8),
                    Instant.now(),
                    AnalyticsType.DATA_COMPLETENESS,
                    metrics.build(),
                    "Comprehensive data quality analytics including completeness, freshness, and consistency"
            ));
            
        } catch (Exception e) {
            log.error("Error collecting data quality analytics: {}", e.getMessage(), e);
            throw new AnalyticsCollectionException("DATA_QUALITY", e.getMessage(), e);
        }
    }
    
    /**
     * Collects data completeness metrics.
     */
    private void collectDataCompletenessMetrics(Dataset<Row> ratingsDf, AnalyticsMetrics metrics) {
        log.debug("Collecting data completeness metrics...");
        
        // Calculate completeness metrics
        long totalRows = ratingsDf.count();
        long nonNullUserIds = ratingsDf.filter(functions.col("userId").isNotNull()).count();
        long nonNullMovieIds = ratingsDf.filter(functions.col("movieId").isNotNull()).count();
        long nonNullRatings = ratingsDf.filter(functions.col("ratingActual").isNotNull()).count();
        long nonNullTimestamps = ratingsDf.filter(functions.col("timestampEpochSeconds").isNotNull()).count();

        // Calculate completeness percentages
        double userIdCompleteness = (double) nonNullUserIds / totalRows * 100;
        double movieIdCompleteness = (double) nonNullMovieIds / totalRows * 100;
        double ratingCompleteness = (double) nonNullRatings / totalRows * 100;
        double timestampCompleteness = (double) nonNullTimestamps / totalRows * 100;

        metrics.addMetric("completeness_totalRows", totalRows)
               .addMetric("completeness_userIdCompleteness", userIdCompleteness)
               .addMetric("completeness_movieIdCompleteness", movieIdCompleteness)
               .addMetric("completeness_ratingCompleteness", ratingCompleteness)
               .addMetric("completeness_timestampCompleteness", timestampCompleteness)
               .addMetric("completeness_overallCompleteness", 
                       (userIdCompleteness + movieIdCompleteness + ratingCompleteness + timestampCompleteness) / 4);

        // Check for duplicate records
        long uniqueRecords = ratingsDf.dropDuplicates().count();
        long duplicateRecords = totalRows - uniqueRecords;
        
        metrics.addMetric("completeness_uniqueRecords", uniqueRecords)
               .addMetric("completeness_duplicateRecords", duplicateRecords)
               .addMetric("completeness_duplicatePercentage", (double) duplicateRecords / totalRows * 100);
        
        log.debug("Data completeness metrics collected");
    }
    
    /**
     * Collects data freshness metrics.
     */
    private void collectDataFreshnessMetrics(Dataset<Row> ratingsDf, AnalyticsMetrics metrics) {
        log.debug("Collecting data freshness metrics...");
        
        // Calculate freshness metrics based on timestamp
        Row freshnessStats = ratingsDf.agg(
                functions.min("timestampEpochSeconds").as("oldestRating"),
                functions.max("timestampEpochSeconds").as("newestRating"),
                functions.count("timestampEpochSeconds").as("totalRatings")
        ).first();

        long oldestTimestamp = freshnessStats.getLong(0);
        long newestTimestamp = freshnessStats.getLong(1);
        long totalRatings = freshnessStats.getLong(2);

        // Calculate time spans
        long timeSpanSeconds = newestTimestamp - oldestTimestamp;
        long timeSpanDays = timeSpanSeconds / (24 * 60 * 60);

        // Calculate ratings per time period
        double ratingsPerDay = totalRatings / (double) Math.max(timeSpanDays, 1);
        double ratingsPerHour = ratingsPerDay / 24;

        metrics.addMetric("freshness_oldestRatingTimestamp", oldestTimestamp)
               .addMetric("freshness_newestRatingTimestamp", newestTimestamp)
               .addMetric("freshness_timeSpanDays", timeSpanDays)
               .addMetric("freshness_ratingsPerDay", ratingsPerDay)
               .addMetric("freshness_ratingsPerHour", ratingsPerHour);

        // Calculate recent activity (last 30 days equivalent in seconds)
        long thirtyDaysAgo = newestTimestamp - (30L * 24 * 60 * 60);
        long recentRatings = ratingsDf.filter(functions.col("timestampEpochSeconds").gt(thirtyDaysAgo)).count();
        
        metrics.addMetric("freshness_recentRatings30Days", recentRatings)
               .addMetric("freshness_recentActivityPercentage", (double) recentRatings / totalRatings * 100);
        
        log.debug("Data freshness metrics collected");
    }
    
    /**
     * Collects data consistency metrics.
     */
    private void collectDataConsistencyMetrics(Dataset<Row> ratingsDf, Dataset<Row> moviesData, 
                                             Dataset<Row> tagsData, AnalyticsMetrics metrics) {
        log.debug("Collecting data consistency metrics...");
        
        // Check rating value consistency
        Row ratingStats = ratingsDf.agg(
                functions.min("ratingActual").as("minRating"),
                functions.max("ratingActual").as("maxRating"),
                functions.countDistinct("ratingActual").as("uniqueRatingValues")
        ).first();
        
        double minRating = ratingStats.getDouble(0);
        double maxRating = ratingStats.getDouble(1);
        long uniqueRatingValues = ratingStats.getLong(2);
        
        // Check if ratings are within expected range (typically 0.5 to 5.0 for MovieLens)
        boolean ratingsInValidRange = minRating >= 0.5 && maxRating <= 5.0;
        
        metrics.addMetric("consistency_minRating", minRating)
               .addMetric("consistency_maxRating", maxRating)
               .addMetric("consistency_uniqueRatingValues", uniqueRatingValues)
               .addMetric("consistency_ratingsInValidRange", ratingsInValidRange);
        
        // Check for orphaned records if movies data is available
        if (moviesData != null && !moviesData.isEmpty()) {
            long ratingsWithMovies = ratingsDf.join(moviesData, "movieId").count();
            long totalRatings = ratingsDf.count();
            long orphanedRatings = totalRatings - ratingsWithMovies;
            
            metrics.addMetric("consistency_ratingsWithMovieData", ratingsWithMovies)
                   .addMetric("consistency_orphanedRatings", orphanedRatings)
                   .addMetric("consistency_orphanedPercentage", (double) orphanedRatings / totalRatings * 100);
        }
        
        // Check for orphaned tags if tags data is available
        if (tagsData != null && !tagsData.isEmpty()) {
            long tagsWithRatings = tagsData.join(ratingsDf, 
                    tagsData.col("movieId").equalTo(ratingsDf.col("movieId"))
                    .and(tagsData.col("userId").equalTo(ratingsDf.col("userId")))).count();
            long totalTags = tagsData.count();
            long orphanedTags = totalTags - tagsWithRatings;
            
            metrics.addMetric("consistency_tagsWithRatingData", tagsWithRatings)
                   .addMetric("consistency_orphanedTags", orphanedTags)
                   .addMetric("consistency_orphanedTagsPercentage", (double) orphanedTags / totalTags * 100);
        }
        
        // Check timestamp consistency
        long futureTimestamps = ratingsDf.filter(
                functions.col("timestampEpochSeconds").gt(System.currentTimeMillis() / 1000)).count();
        long invalidTimestamps = ratingsDf.filter(
                functions.col("timestampEpochSeconds").lt(0)).count();
        
        metrics.addMetric("consistency_futureTimestamps", futureTimestamps)
               .addMetric("consistency_invalidTimestamps", invalidTimestamps);
        
        // Calculate overall consistency score
        double consistencyScore = 100.0;
        if (!ratingsInValidRange) consistencyScore -= 20;
        if (futureTimestamps > 0) consistencyScore -= 10;
        if (invalidTimestamps > 0) consistencyScore -= 10;
        
        metrics.addMetric("consistency_overallScore", consistencyScore);
        
        log.debug("Data consistency metrics collected");
    }
    
    @Override
    public String getAnalyticsType() {
        return "DATA_QUALITY";
    }
    
    @Override
    public boolean canProcess(Dataset<Row> ratingsDf, Dataset<Row> moviesData, Dataset<Row> tagsData) {
        return ratingsDf != null && !ratingsDf.isEmpty();
    }
    
    @Override
    public int getPriority() {
        return 5; // Very high priority for data quality
    }
}
