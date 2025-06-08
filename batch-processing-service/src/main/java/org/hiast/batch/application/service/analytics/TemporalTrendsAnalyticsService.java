package org.hiast.batch.application.service.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
// import org.hiast.batch.application.pipeline.BasePipelineContext; // Not used
import org.hiast.batch.domain.exception.AnalyticsCollectionException;
import org.hiast.model.AnalyticsType;
import org.hiast.model.DataAnalytics;
import org.hiast.model.analytics.AnalyticsMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList; // Import ArrayList
import java.util.List;
import java.util.UUID;

/**
 * Service responsible for collecting temporal trends analytics.
 * Analyzes rating patterns over time to identify seasonal trends,
 * peak usage periods, and temporal user behavior patterns.
 *
 * This service provides insights into when users are most active
 * and how rating patterns change over different time periods.
 * OPTIMIZED VERSION: Robust numeric getters and column pruning.
 */
public class TemporalTrendsAnalyticsService implements AnalyticsCollector {

    private static final Logger log = LoggerFactory.getLogger(TemporalTrendsAnalyticsService.class);

    // Helper method for robustly getting a double value from a Row
    private double getDoubleFromRow(Row row, String fieldName, double defaultValue) {
        try {
            int fieldIndex = row.fieldIndex(fieldName);
            if (row.isNullAt(fieldIndex)) {
                return defaultValue;
            }
            Object value = row.get(fieldIndex);
            if (value instanceof Number) {
                return ((Number) value).doubleValue();
            }
            return Double.parseDouble(value.toString());
        } catch (Exception e) {
            log.warn("Failed to get double for field '{}' from row. Value: '{}'. Error: {}. Returning default: {}",
                    fieldName, row.getAs(fieldName), e.getMessage(), defaultValue);
            return defaultValue;
        }
    }

    // Helper method for robustly getting a long value from a Row
    private long getLongFromRow(Row row, String fieldName, long defaultValue) {
        try {
            int fieldIndex = row.fieldIndex(fieldName);
            if (row.isNullAt(fieldIndex)) {
                return defaultValue;
            }
            Object value = row.get(fieldIndex);
            if (value instanceof Number) {
                return ((Number) value).longValue();
            }
            return Long.parseLong(value.toString());
        } catch (Exception e) {
            log.warn("Failed to get long for field '{}' from row. Value: '{}'. Error: {}. Returning default: {}",
                    fieldName, row.getAs(fieldName), e.getMessage(), defaultValue);
            return defaultValue;
        }
    }

    // Helper method for robustly getting an int value from a Row
    private int getIntFromRow(Row row, String fieldName, int defaultValue) {
        try {
            int fieldIndex = row.fieldIndex(fieldName);
            if (row.isNullAt(fieldIndex)) {
                return defaultValue;
            }
            Object value = row.get(fieldIndex);
            if (value instanceof Number) {
                return ((Number) value).intValue();
            }
            return Integer.parseInt(value.toString());
        } catch (Exception e) {
            log.warn("Failed to get int for field '{}' from row. Value: '{}'. Error: {}. Returning default: {}",
                    fieldName, row.getAs(fieldName), e.getMessage(), defaultValue);
            return defaultValue;
        }
    }

    @Override
    public List<DataAnalytics> collectAnalytics(Dataset<Row> ratingsDf,
                                                Dataset<Row> moviesData,
                                                Dataset<Row> tagsData) {

        if (!canProcess(ratingsDf, moviesData, tagsData)) {
            throw new AnalyticsCollectionException("TEMPORAL_TRENDS", "Insufficient data for temporal trends analytics");
        }
        List<DataAnalytics> collectedAnalytics = new ArrayList<>();

        try {
            log.info("Collecting temporal trends analytics (Optimized)...");
            AnalyticsMetrics metrics = AnalyticsMetrics.builder();

            // --- OPTIMIZATION: Select only necessary columns from ratingsDf ---
            Dataset<Row> relevantRatingsForTemporal = ratingsDf.select(
                    functions.col("timestampEpochSeconds"),
                    functions.col("ratingActual"),
                    functions.col("userId") // For countDistinct("userId") in yearly trends
            );

            Dataset<Row> temporalData = relevantRatingsForTemporal
                    .withColumn("date", functions.from_unixtime(functions.col("timestampEpochSeconds")))
                    .withColumn("year", functions.year(functions.col("date"))) // Use derived "date" column
                    .withColumn("month", functions.month(functions.col("date")))
                    .withColumn("dayOfWeek", functions.dayofweek(functions.col("date")))
                    .withColumn("hour", functions.hour(functions.col("date")));

            // Persist temporalData as it's used by multiple sub-methods
            // temporalData.persist(StorageLevel.MEMORY_AND_DISK_SER());
            // log.debug("Persisted temporalData for analytics. Count: {}", temporalData.count());


            collectYearlyTrends(temporalData, metrics);
            collectMonthlyTrends(temporalData, metrics);
            collectWeeklyTrends(temporalData, metrics);
            collectHourlyTrends(temporalData, metrics);

            // temporalData.unpersist();
            // log.debug("Unpersisted temporalData.");

            log.info("Temporal trends analytics collection completed with {} metrics", metrics.size());

            collectedAnalytics.add(new DataAnalytics(
                    "temporal_trends_" + UUID.randomUUID().toString().substring(0, 8),
                    Instant.now(),
                    AnalyticsType.TEMPORAL_TRENDS,
                    metrics.build(),
                    "Temporal trends and seasonal pattern analytics"
            ));
            return collectedAnalytics;

        } catch (Exception e) {
            log.error("Error collecting temporal trends analytics: {}", e.getMessage(), e);
            throw new AnalyticsCollectionException("TEMPORAL_TRENDS", e.getMessage(), e);
        }
    }

    private void collectYearlyTrends(Dataset<Row> temporalData, AnalyticsMetrics metrics) {
        log.debug("Collecting yearly trends...");
        // temporalData already has selected columns.
        Dataset<Row> yearlyTrends = temporalData.groupBy("year")
                .agg(
                        functions.count("ratingActual").as("ratingCount"),
                        functions.avg("ratingActual").as("avgRating"),
                        functions.countDistinct("userId").as("activeUsers")
                )
                .orderBy("year"); // Result is small (number of years)

        List<Row> collectedYearlyTrends = yearlyTrends.collectAsList();
        for(Row row : collectedYearlyTrends) {
            int year = getIntFromRow(row, "year", 0);
            metrics.addMetric("year_" + year + "_count", getLongFromRow(row, "ratingCount", 0L))
                    .addMetric("year_" + year + "_avgRating", getDoubleFromRow(row, "avgRating", 0.0))
                    .addMetric("year_" + year + "_activeUsers", getLongFromRow(row, "activeUsers", 0L));
        }
        log.debug("Yearly trends collected");
    }

    private void collectMonthlyTrends(Dataset<Row> temporalData, AnalyticsMetrics metrics) {
        log.debug("Collecting monthly trends...");
        Dataset<Row> monthlyTrends = temporalData.groupBy("month")
                .agg(
                        functions.count("ratingActual").as("ratingCount"),
                        functions.avg("ratingActual").as("avgRating")
                )
                .orderBy("month"); // Result is small (12 rows)

        List<Row> collectedMonthlyTrends = monthlyTrends.collectAsList();
        for(Row row : collectedMonthlyTrends) {
            int month = getIntFromRow(row, "month", 0);
            metrics.addMetric("month_" + month + "_count", getLongFromRow(row, "ratingCount", 0L))
                    .addMetric("month_" + month + "_avgRating", getDoubleFromRow(row, "avgRating", 0.0));
        }
        log.debug("Monthly trends collected");
    }

    private void collectWeeklyTrends(Dataset<Row> temporalData, AnalyticsMetrics metrics) {
        log.debug("Collecting weekly trends...");
        Dataset<Row> weeklyTrends = temporalData.groupBy("dayOfWeek")
                .agg(
                        functions.count("ratingActual").as("ratingCount"),
                        functions.avg("ratingActual").as("avgRating")
                )
                .orderBy("dayOfWeek"); // Result is small (7 rows)

        List<Row> collectedWeeklyTrends = weeklyTrends.collectAsList();
        for(Row row : collectedWeeklyTrends) {
            int dayOfWeek = getIntFromRow(row, "dayOfWeek", 0);
            metrics.addMetric("dayOfWeek_" + dayOfWeek + "_count", getLongFromRow(row, "ratingCount", 0L))
                    .addMetric("dayOfWeek_" + dayOfWeek + "_avgRating", getDoubleFromRow(row, "avgRating", 0.0));
        }
        log.debug("Weekly trends collected");
    }

    private void collectHourlyTrends(Dataset<Row> temporalData, AnalyticsMetrics metrics) {
        log.debug("Collecting hourly trends...");
        Dataset<Row> hourlyTrends = temporalData.groupBy("hour")
                .agg(
                        functions.count("ratingActual").as("ratingCount"),
                        functions.avg("ratingActual").as("avgRating")
                )
                .orderBy(functions.desc("ratingCount")); // Order by count to get top active hours

        // Add hourly trends (top 5 most active hours)
        List<Row> collectedHourlyTrends = hourlyTrends.limit(5).collectAsList(); // Result is small (5 rows)
        for(Row row : collectedHourlyTrends) {
            int hour = getIntFromRow(row, "hour", 0);
            metrics.addMetric("topHour_" + hour + "_count", getLongFromRow(row, "ratingCount", 0L))
                    .addMetric("topHour_" + hour + "_avgRating", getDoubleFromRow(row, "avgRating", 0.0));
        }
        log.debug("Hourly trends collected");
    }

    @Override
    public String getAnalyticsType() {
        return "TEMPORAL_TRENDS";
    }

    @Override
    public boolean canProcess(Dataset<Row> ratingsDf, Dataset<Row> moviesData, Dataset<Row> tagsData) {
        return ratingsDf != null && !ratingsDf.isEmpty() && ratingsDf.columns().length >0 && java.util.Arrays.asList(ratingsDf.columns()).contains("timestampEpochSeconds");
    }

    @Override
    public int getPriority() {
        return 40;
    }
}
