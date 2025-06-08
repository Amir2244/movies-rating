package org.hiast.batch.application.service.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.storage.StorageLevel;
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
 * Service responsible for collecting user engagement analytics.
 * Analyzes user engagement depth, rating variance, and interaction patterns
 * to understand how deeply users engage with the platform.
 *
 * This service generates a separate analytics document focused specifically
 * on user engagement metrics.
 * OPTIMIZED VERSION: Robust numeric getters and column pruning.
 */
public class UserEngagementAnalyticsService implements AnalyticsCollector {

    private static final Logger log = LoggerFactory.getLogger(UserEngagementAnalyticsService.class);

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

    @Override
    public List<DataAnalytics> collectAnalytics(Dataset<Row> ratingsDf,
                                                Dataset<Row> moviesData,
                                                Dataset<Row> tagsData) {

        if (!canProcess(ratingsDf, moviesData, tagsData)) {
            throw new AnalyticsCollectionException("USER_ENGAGEMENT", "Insufficient data for user engagement analytics");
        }
        List<DataAnalytics> collectedAnalytics = new ArrayList<>();

        try {
            log.info("Collecting user engagement analytics (Optimized)...");
            AnalyticsMetrics metrics = AnalyticsMetrics.builder();

            collectUserEngagementMetricsInternal(ratingsDf, metrics);

            log.info("User engagement analytics collection completed with {} metrics", metrics.size());

            collectedAnalytics.add(new DataAnalytics(
                    "user_engagement_" + UUID.randomUUID().toString().substring(0, 8),
                    Instant.now(),
                    AnalyticsType.USER_ENGAGEMENT,
                    metrics.build(),
                    "User engagement depth and interaction pattern analytics"
            ));
            return collectedAnalytics;

        } catch (Exception e) {
            log.error("Error collecting user engagement analytics: {}", e.getMessage(), e);
            throw new AnalyticsCollectionException("USER_ENGAGEMENT", e.getMessage(), e);
        }
    }

    /**
     * Internal method to collect user engagement depth metrics.
     */
    private void collectUserEngagementMetricsInternal(Dataset<Row> ratingsDf, AnalyticsMetrics metrics) {
        log.debug("Executing internal user engagement metrics collection...");

        // --- OPTIMIZATION: Select only necessary columns from ratingsDf ---
        Dataset<Row> relevantRatings = ratingsDf.select(
                functions.col("userId"),
                functions.col("movieId"),
                functions.col("ratingActual"),
                functions.col("timestampEpochSeconds")
        );

        Dataset<Row> userEngagement = relevantRatings.groupBy("userId")
                .agg(
                        functions.count("ratingActual").as("totalRatings"),
                        functions.avg("ratingActual").as("avgRating"),
                        functions.stddev_samp("ratingActual").as("ratingStdDev"), // Use stddev_samp
                        functions.countDistinct("movieId").as("uniqueMovies"),
                        functions.max("timestampEpochSeconds").minus(functions.min("timestampEpochSeconds")).as("engagementSpan")
                );

        // Persist userEngagement as it's used for multiple aggregations and filters below
        userEngagement.persist(StorageLevel.MEMORY_AND_DISK_SER());
        log.debug("Persisted userEngagement dataset. Count: {}", userEngagement.count());


        Row engagementStats = userEngagement.agg(
                functions.avg("totalRatings").as("avgTotalRatingsPerUser"), // To avoid confusion with per-user totalRatings
                functions.avg("avgRating").as("avgAvgUserRating"),
                functions.avg("ratingStdDev").as("avgRatingStdDev"),
                functions.avg("uniqueMovies").as("avgUniqueMoviesPerUser"),
                functions.avg("engagementSpan").as("avgEngagementSpan")
        ).first();

        if (engagementStats != null) {
            metrics.addMetric("avgRatingsPerUser", getDoubleFromRow(engagementStats, "avgTotalRatingsPerUser", 0.0))
                    .addMetric("avgUserRating", getDoubleFromRow(engagementStats, "avgAvgUserRating", 0.0))
                    .addMetricWithDefault("avgRatingStdDev", engagementStats.get(engagementStats.fieldIndex("avgRatingStdDev")), 0.0) // Use get() for objects that might be null
                    .addMetric("avgUniqueMoviesPerUser", getDoubleFromRow(engagementStats, "avgUniqueMoviesPerUser", 0.0))
                    .addMetric("avgEngagementSpanSeconds", getDoubleFromRow(engagementStats, "avgEngagementSpan", 0.0));
        }

        long highlyEngagedUsers = userEngagement.filter(functions.col("totalRatings").gt(50)).count();
        long moderatelyEngagedUsers = userEngagement.filter(functions.col("totalRatings").between(10, 50)).count();
        long lowEngagedUsers = userEngagement.filter(functions.col("totalRatings").lt(10)).count();

        metrics.addMetric("highlyEngagedUsers", highlyEngagedUsers)
                .addMetric("moderatelyEngagedUsers", moderatelyEngagedUsers)
                .addMetric("lowEngagedUsers", lowEngagedUsers);

        long totalUsers = userEngagement.count(); // This reuses the persisted userEngagement
        metrics.addMetric("totalEngagedUsers", totalUsers);
        if (totalUsers > 0) {
            double highEngagementPercentage = (double) highlyEngagedUsers / totalUsers * 100.0;
            double moderateEngagementPercentage = (double) moderatelyEngagedUsers / totalUsers * 100.0;
            double lowEngagementPercentage = (double) lowEngagedUsers / totalUsers * 100.0;
            metrics.addMetric("highEngagementPercentage", highEngagementPercentage)
                    .addMetric("moderateEngagementPercentage", moderateEngagementPercentage)
                    .addMetric("lowEngagementPercentage", lowEngagementPercentage);
        } else {
            metrics.addMetric("highEngagementPercentage", 0.0)
                    .addMetric("moderateEngagementPercentage", 0.0)
                    .addMetric("lowEngagementPercentage", 0.0);
        }


        Dataset<Row> ratingBehavior = userEngagement // Reuses persisted userEngagement
                .withColumn("ratingFrequency",
                        functions.when(functions.col("totalRatings").gt(100), "VeryActive")
                                .when(functions.col("totalRatings").gt(50), "Active")
                                .when(functions.col("totalRatings").gt(10), "Moderate")
                                .otherwise("Casual"))
                .withColumn("ratingDiversity",
                        functions.when(functions.col("uniqueMovies").gt(functions.col("totalRatings").multiply(0.8)), "HighDiversity")
                                .when(functions.col("uniqueMovies").gt(functions.col("totalRatings").multiply(0.5)), "ModerateDiversity")
                                .otherwise("LowDiversity"));

        Dataset<Row> behaviorCounts = ratingBehavior
                .groupBy("ratingFrequency", "ratingDiversity")
                .count()
                .orderBy("ratingFrequency", "ratingDiversity"); // Small result

        List<Row> collectedBehaviorCounts = behaviorCounts.collectAsList();
        for(Row row : collectedBehaviorCounts) {
            String frequency = row.getString(row.fieldIndex("ratingFrequency"));
            String diversity = row.getString(row.fieldIndex("ratingDiversity"));
            long count = getLongFromRow(row, "count", 0L);
            String behaviorKey = "behavior_" + frequency + "_" + diversity;
            metrics.addMetric(behaviorKey, count);
        }

        Row qualityStats = userEngagement.agg( // Reuses persisted userEngagement
                functions.expr("percentile_approx(totalRatings, 0.5)").as("medianRatingsPerUser"), // Use percentile_approx
                functions.expr("percentile_approx(uniqueMovies, 0.5)").as("medianUniqueMoviesPerUser"),
                functions.max("totalRatings").as("maxRatingsPerUser"),
                functions.max("uniqueMovies").as("maxUniqueMoviesPerUser")
        ).first();

        if (qualityStats != null) {
            metrics.addMetric("medianRatingsPerUser", getDoubleFromRow(qualityStats, "medianRatingsPerUser", 0.0))
                    .addMetric("medianUniqueMoviesPerUser", getDoubleFromRow(qualityStats, "medianUniqueMoviesPerUser", 0.0))
                    .addMetric("maxRatingsPerUser", getLongFromRow(qualityStats, "maxRatingsPerUser", 0L))
                    .addMetric("maxUniqueMoviesPerUser", getLongFromRow(qualityStats, "maxUniqueMoviesPerUser", 0L));
        }

        double engagementConsistency = 100.0;
        // Use the already fetched engagementStats row
        if (engagementStats != null && !engagementStats.isNullAt(engagementStats.fieldIndex("avgRatingStdDev"))) {
            double avgVariance = getDoubleFromRow(engagementStats, "avgRatingStdDev", 0.0); // Use the correct field name
            if (avgVariance > 2.0) engagementConsistency -= 30;
            else if (avgVariance > 1.5) engagementConsistency -= 20;
            else if (avgVariance > 1.0) engagementConsistency -= 10;
        }
        metrics.addMetric("engagementConsistencyScore", engagementConsistency);

        userEngagement.unpersist(); // Unpersist after all uses
        log.debug("Unpersisted userEngagement dataset.");
    }

    @Override
    public String getAnalyticsType() {
        return "USER_ENGAGEMENT";
    }

    @Override
    public boolean canProcess(Dataset<Row> ratingsDf, Dataset<Row> moviesData, Dataset<Row> tagsData) {
        if (ratingsDf == null || ratingsDf.isEmpty()) return false;
        List<String> requiredColumns = java.util.Arrays.asList("userId", "movieId", "ratingActual", "timestampEpochSeconds");
        List<String> actualColumns = java.util.Arrays.asList(ratingsDf.columns());
        return actualColumns.containsAll(requiredColumns);
    }

    @Override
    public int getPriority() {
        return 25;
    }
}
