package org.hiast.batch.application.service.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hiast.batch.application.pipeline.BasePipelineContext;
import org.hiast.batch.domain.exception.AnalyticsCollectionException;
import org.hiast.batch.domain.model.AnalyticsType;
import org.hiast.batch.domain.model.DataAnalytics;
import org.hiast.batch.domain.model.analytics.AnalyticsMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Service responsible for collecting data freshness analytics.
 * Analyzes the recency and temporal distribution of data to understand
 * data currency and activity patterns over time.
 * 
 * This service generates a separate analytics document focused specifically
 * on data freshness metrics, exactly as in the original implementation.
 */
public class DataFreshnessAnalyticsService implements AnalyticsCollector {
    
    private static final Logger log = LoggerFactory.getLogger(DataFreshnessAnalyticsService.class);
    
    @Override
    public List<DataAnalytics> collectAnalytics(Dataset<Row> ratingsDf,
                                                Dataset<Row> moviesData,
                                                Dataset<Row> tagsData) {
        
        if (!canProcess(ratingsDf, moviesData, tagsData)) {
            throw new AnalyticsCollectionException("DATA_FRESHNESS", "Insufficient data for data freshness analytics");
        }
        
        try {
            log.info("Collecting data freshness analytics...");
            
            AnalyticsMetrics metrics = AnalyticsMetrics.builder();
            
            // Collect data freshness metrics - exact same as original
            collectDataFreshnessMetrics(ratingsDf, metrics);
            
            log.info("Data freshness analytics collection completed with {} metrics", metrics.size());
            
            return Collections.singletonList(new DataAnalytics(
                    "data_freshness_" + UUID.randomUUID().toString().substring(0, 8),
                    Instant.now(),
                    AnalyticsType.DATA_FRESHNESS,
                    metrics.build(),
                    "Data freshness and recency analytics"
            ));
            
        } catch (Exception e) {
            log.error("Error collecting data freshness analytics: {}", e.getMessage(), e);
            throw new AnalyticsCollectionException("DATA_FRESHNESS", e.getMessage(), e);
        }
    }
    
    /**
     * Collects data freshness metrics - exact same as original implementation.
     */
    private void collectDataFreshnessMetrics(Dataset<Row> ratingsDf, AnalyticsMetrics metrics) {
        // log.info("Collecting data freshness analytics..."); // Redundant, already logged in main method

        try {
            // Calculate freshness metrics based on timestamp - exact same as original
            Row freshnessStats = ratingsDf.agg(
                    functions.min("timestampEpochSeconds").as("oldestRating"),
                    functions.max("timestampEpochSeconds").as("newestRating"),
                    functions.count("timestampEpochSeconds").as("totalRatings")
            ).first();

            long oldestTimestamp = freshnessStats.getLong(0);
            long newestTimestamp = freshnessStats.getLong(1);
            long totalRatings = freshnessStats.getLong(2);

            // Calculate time spans - exact same as original
            long timeSpanSeconds = newestTimestamp - oldestTimestamp;
            long timeSpanDays = timeSpanSeconds / (24 * 60 * 60);

            // Calculate ratings per time period - exact same as original
            double ratingsPerDay = totalRatings / (double) Math.max(timeSpanDays, 1);
            double ratingsPerHour = ratingsPerDay / 24;

            metrics.addMetric("oldestRatingTimestamp", oldestTimestamp)
                   .addMetric("newestRatingTimestamp", newestTimestamp)
                   .addMetric("timeSpanDays", timeSpanDays)
                   .addMetric("ratingsPerDay", ratingsPerDay)
                   .addMetric("ratingsPerHour", ratingsPerHour);

            // Calculate recent activity (last 30 days equivalent in seconds) - exact same as original
            long thirtyDaysAgo = newestTimestamp - (30L * 24 * 60 * 60);
            long recentRatings = ratingsDf.filter(functions.col("timestampEpochSeconds").gt(thirtyDaysAgo)).count();
            
            metrics.addMetric("recentRatings30Days", recentRatings)
                   .addMetric("recentActivityPercentage", (double) recentRatings / totalRatings * 100);

            // Additional freshness insights - exact same as original
            long sevenDaysAgo = newestTimestamp - (7L * 24 * 60 * 60);
            long oneDayAgo = newestTimestamp - (24 * 60 * 60);
            
            long ratingsLast7Days = ratingsDf.filter(functions.col("timestampEpochSeconds").gt(sevenDaysAgo)).count();
            long ratingsLast24Hours = ratingsDf.filter(functions.col("timestampEpochSeconds").gt(oneDayAgo)).count();
            
            metrics.addMetric("ratingsLast7Days", ratingsLast7Days)
                   .addMetric("ratingsLast24Hours", ratingsLast24Hours)
                   .addMetric("weeklyActivityPercentage", (double) ratingsLast7Days / totalRatings * 100)
                   .addMetric("dailyActivityPercentage", (double) ratingsLast24Hours / totalRatings * 100);

            // Data age analysis - exact same as original
            long currentTime = System.currentTimeMillis() / 1000;
            long dataAgeSeconds = currentTime - newestTimestamp;
            long dataAgeDays = dataAgeSeconds / (24 * 60 * 60);
            
            metrics.addMetric("dataAgeSeconds", dataAgeSeconds)
                   .addMetric("dataAgeDays", dataAgeDays);

            // Freshness score calculation - exact same as original
            double freshnessScore = 100.0;
            if (dataAgeDays > 365) freshnessScore -= 50; // Very old data
            else if (dataAgeDays > 90) freshnessScore -= 30; // Somewhat old data
            else if (dataAgeDays > 30) freshnessScore -= 15; // Moderately old data
            else if (dataAgeDays > 7) freshnessScore -= 5; // Slightly old data
            
            metrics.addMetric("freshnessScore", freshnessScore);

            // Activity trend analysis - exact same as original
            String activityTrend;
            if (ratingsLast24Hours > ratingsLast7Days / 7.0) {
                activityTrend = "Increasing";
            } else if (ratingsLast24Hours < ratingsLast7Days / 14.0) {
                activityTrend = "Decreasing";
            } else {
                activityTrend = "Stable";
            }
            
            metrics.addMetric("activityTrend", activityTrend);

        } catch (Exception e) {
            log.error("Error collecting data freshness analytics: {}", e.getMessage(), e);
            throw new AnalyticsCollectionException("DATA_FRESHNESS", e.getMessage(), e);
        }
    }
    
    @Override
    public String getAnalyticsType() {
        return "DATA_FRESHNESS";
    }
    
    @Override
    public boolean canProcess(Dataset<Row> ratingsDf, Dataset<Row> moviesData, Dataset<Row> tagsData) {
        return ratingsDf != null && !ratingsDf.isEmpty();
    }
    
    @Override
    public int getPriority() {
        return 15; // High priority for data freshness monitoring
    }
}
