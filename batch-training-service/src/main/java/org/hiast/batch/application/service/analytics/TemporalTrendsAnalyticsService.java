package org.hiast.batch.application.service.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hiast.batch.application.pipeline.ALSTrainingPipelineContext;
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
 * Service responsible for collecting temporal trends analytics.
 * Analyzes rating patterns over time to identify seasonal trends,
 * peak usage periods, and temporal user behavior patterns.
 * 
 * This service provides insights into when users are most active
 * and how rating patterns change over different time periods.
 */
public class TemporalTrendsAnalyticsService implements AnalyticsCollector {
    
    private static final Logger log = LoggerFactory.getLogger(TemporalTrendsAnalyticsService.class);
    
    @Override
    public List<DataAnalytics> collectAnalytics(Dataset<Row> ratingsDf,
                                                Dataset<Row> moviesData,
                                                Dataset<Row> tagsData,
                                                ALSTrainingPipelineContext context) {
        
        if (!canProcess(ratingsDf, moviesData, tagsData)) {
            throw new AnalyticsCollectionException("TEMPORAL_TRENDS", "Insufficient data for temporal trends analytics");
        }
        
        try {
            log.info("Collecting temporal trends analytics...");
            
            AnalyticsMetrics metrics = AnalyticsMetrics.builder();
            
            // Convert timestamp to readable date components - exact same as original
            Dataset<Row> temporalData = ratingsDf
                    .withColumn("date", functions.from_unixtime(functions.col("timestampEpochSeconds")))
                    .withColumn("year", functions.year(functions.from_unixtime(functions.col("timestampEpochSeconds"))))
                    .withColumn("month", functions.month(functions.from_unixtime(functions.col("timestampEpochSeconds"))))
                    .withColumn("dayOfWeek", functions.dayofweek(functions.from_unixtime(functions.col("timestampEpochSeconds"))))
                    .withColumn("hour", functions.hour(functions.from_unixtime(functions.col("timestampEpochSeconds"))));
            
            // Collect temporal trends - exact same as original
            collectYearlyTrends(temporalData, metrics);
            collectMonthlyTrends(temporalData, metrics);
            collectWeeklyTrends(temporalData, metrics);
            collectHourlyTrends(temporalData, metrics);
            
            log.info("Temporal trends analytics collection completed with {} metrics", metrics.size());
            
            return Collections.singletonList(new DataAnalytics(
                    "temporal_trends_" + UUID.randomUUID().toString().substring(0, 8),
                    Instant.now(),
                    AnalyticsType.TEMPORAL_TRENDS,
                    metrics.build(),
                    "Temporal trends and seasonal pattern analytics"
            ));
            
        } catch (Exception e) {
            log.error("Error collecting temporal trends analytics: {}", e.getMessage(), e);
            throw new AnalyticsCollectionException("TEMPORAL_TRENDS", e.getMessage(), e);
        }
    }
    
    /**
     * Collects yearly trends - exact same as original implementation.
     */
    private void collectYearlyTrends(Dataset<Row> temporalData, AnalyticsMetrics metrics) {
        log.debug("Collecting yearly trends...");
        
        // Analyze trends by year - exact same as original
        Dataset<Row> yearlyTrends = temporalData.groupBy("year")
                .agg(
                        functions.count("ratingActual").as("ratingCount"),
                        functions.avg("ratingActual").as("avgRating"),
                        functions.countDistinct("userId").as("activeUsers")
                )
                .orderBy("year");

        // Add yearly trends - exact same keys as original
        yearlyTrends.collectAsList().forEach(row -> {
            int year = row.getInt(0);
            metrics.addMetric("year_" + year + "_count", row.getLong(1))
                   .addMetric("year_" + year + "_avgRating", row.getDouble(2))
                   .addMetric("year_" + year + "_activeUsers", row.getLong(3));
        });
        
        log.debug("Yearly trends collected");
    }
    
    /**
     * Collects monthly trends - exact same as original implementation.
     */
    private void collectMonthlyTrends(Dataset<Row> temporalData, AnalyticsMetrics metrics) {
        log.debug("Collecting monthly trends...");
        
        // Analyze trends by month - exact same as original
        Dataset<Row> monthlyTrends = temporalData.groupBy("month")
                .agg(
                        functions.count("ratingActual").as("ratingCount"),
                        functions.avg("ratingActual").as("avgRating")
                )
                .orderBy("month");

        // Add monthly trends - exact same keys as original
        monthlyTrends.collectAsList().forEach(row -> {
            int month = row.getInt(0);
            metrics.addMetric("month_" + month + "_count", row.getLong(1))
                   .addMetric("month_" + month + "_avgRating", row.getDouble(2));
        });
        
        log.debug("Monthly trends collected");
    }
    
    /**
     * Collects weekly trends - exact same as original implementation.
     */
    private void collectWeeklyTrends(Dataset<Row> temporalData, AnalyticsMetrics metrics) {
        log.debug("Collecting weekly trends...");
        
        // Analyze trends by day of week - exact same as original
        Dataset<Row> weeklyTrends = temporalData.groupBy("dayOfWeek")
                .agg(
                        functions.count("ratingActual").as("ratingCount"),
                        functions.avg("ratingActual").as("avgRating")
                )
                .orderBy("dayOfWeek");

        // Add weekly trends - exact same keys as original
        weeklyTrends.collectAsList().forEach(row -> {
            int dayOfWeek = row.getInt(0);
            metrics.addMetric("dayOfWeek_" + dayOfWeek + "_count", row.getLong(1))
                   .addMetric("dayOfWeek_" + dayOfWeek + "_avgRating", row.getDouble(2));
        });
        
        log.debug("Weekly trends collected");
    }
    
    /**
     * Collects hourly trends - exact same as original implementation.
     */
    private void collectHourlyTrends(Dataset<Row> temporalData, AnalyticsMetrics metrics) {
        log.debug("Collecting hourly trends...");
        
        // Analyze trends by hour - exact same as original
        Dataset<Row> hourlyTrends = temporalData.groupBy("hour")
                .agg(
                        functions.count("ratingActual").as("ratingCount"),
                        functions.avg("ratingActual").as("avgRating")
                )
                .orderBy("hour");

        // Add hourly trends (top 5 most active hours) - exact same as original
        hourlyTrends.limit(5).collectAsList().forEach(row -> {
            int hour = row.getInt(0);
            metrics.addMetric("topHour_" + hour + "_count", row.getLong(1))
                   .addMetric("topHour_" + hour + "_avgRating", row.getDouble(2));
        });
        
        log.debug("Hourly trends collected");
    }
    
    @Override
    public String getAnalyticsType() {
        return "TEMPORAL_TRENDS";
    }
    
    @Override
    public boolean canProcess(Dataset<Row> ratingsDf, Dataset<Row> moviesData, Dataset<Row> tagsData) {
        return ratingsDf != null && !ratingsDf.isEmpty();
    }
    
    @Override
    public int getPriority() {
        return 40; // Medium priority for temporal trends
    }
}
