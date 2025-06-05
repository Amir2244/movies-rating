package org.hiast.batch.application.service.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.storage.StorageLevel;
// import org.hiast.batch.application.pipeline.BasePipelineContext; // Not used
import org.hiast.batch.domain.exception.AnalyticsCollectionException;
import org.hiast.batch.domain.model.AnalyticsType;
import org.hiast.batch.domain.model.DataAnalytics;
import org.hiast.batch.domain.model.analytics.AnalyticsMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

/**
 * Service responsible for collecting user-related analytics.
 * Handles user activity, engagement, and segmentation analytics.
 *
 * This service follows the Single Responsibility Principle by focusing
 * only on user-related analytics collection.
 * OPTIMIZED VERSION: Robust numeric getters and column pruning.
 */
public class UserAnalyticsService implements AnalyticsCollector {

    private static final Logger log = LoggerFactory.getLogger(UserAnalyticsService.class);

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
            throw new AnalyticsCollectionException("USER_ACTIVITY", "Insufficient data for user analytics");
        }
        List<DataAnalytics> collectedAnalytics = new ArrayList<>();

        try {
            log.info("Collecting user activity analytics (Optimized)...");
            AnalyticsMetrics metrics = AnalyticsMetrics.builder();

            // --- OPTIMIZATION: Persist ratingsDf if it's not already, as it's used by all sub-methods ---
            // This should ideally be handled by the pipeline orchestrator.
            // boolean ratingsDfWasCached = ratingsDf.storageLevel().useMemory();
            // if (!ratingsDfWasCached) {
            //     ratingsDf.persist(StorageLevel.MEMORY_AND_DISK_SER());
            //     log.debug("Persisted ratingsDf for UserAnalyticsService. Count: {}", ratingsDf.count());
            // }


            // --- OPTIMIZATION: Create a common base for user-level aggregations once ---
            // Select only columns needed by ANY of the sub-methods that group by userId
            Dataset<Row> userAggBaseInput = ratingsDf.select(
                    functions.col("userId"),
                    functions.col("movieId"),
                    functions.col("ratingActual"),
                    functions.col("timestampEpochSeconds")
            );

            Dataset<Row> userAggregatedStats = userAggBaseInput.groupBy("userId")
                    .agg(
                            functions.count("ratingActual").as("totalRatings"),          // For activity, engagement, segmentation
                            functions.avg("ratingActual").as("avgRating"),               // For engagement, segmentation
                            functions.stddev_samp("ratingActual").as("ratingStdDev"),    // For engagement, segmentation (use stddev_samp)
                            functions.countDistinct("movieId").as("uniqueMovies"),       // For engagement, segmentation
                            functions.min("timestampEpochSeconds").as("minTimestamp"),   // For engagement (engagementSpan)
                            functions.max("timestampEpochSeconds").as("maxTimestamp")    // For engagement (engagementSpan)
                    )
                    .withColumn("engagementSpan", functions.col("maxTimestamp").minus(functions.col("minTimestamp")));

            // Persist this aggregated DataFrame as it's used by multiple sub-methods
            userAggregatedStats.persist(StorageLevel.MEMORY_AND_DISK_SER());
            log.debug("Persisted userAggregatedStats. Count: {}", userAggregatedStats.count());


            collectUserActivityMetrics(userAggregatedStats, metrics); // Pass the aggregated DF
            collectUserEngagementMetrics(userAggregatedStats, metrics); // Pass the aggregated DF
            collectUserSegmentationMetrics(userAggregatedStats, metrics); // Pass the aggregated DF

            userAggregatedStats.unpersist(); // Unpersist after use
            log.debug("Unpersisted userAggregatedStats.");

            // if (!ratingsDfWasCached) {
            //     ratingsDf.unpersist();
            //     log.debug("Unpersisted ratingsDf in UserAnalyticsService.");
            // }

            log.info("User analytics collection completed with {} metrics", metrics.size());

            collectedAnalytics.add(new DataAnalytics(
                    "user_activity_" + UUID.randomUUID().toString().substring(0, 8),
                    Instant.now(),
                    AnalyticsType.USER_ACTIVITY,
                    metrics.build(),
                    "Comprehensive user activity, engagement, and segmentation analytics"
            ));
            return collectedAnalytics;

        } catch (Exception e) {
            log.error("Error collecting user analytics: {}", e.getMessage(), e);
            throw new AnalyticsCollectionException("USER_ACTIVITY", e.getMessage(), e);
        }
    }

    /**
     * Collects basic user activity metrics from pre-aggregated data.
     */
    private void collectUserActivityMetrics(Dataset<Row> userAggregatedStats, AnalyticsMetrics metrics) {
        log.debug("Collecting basic user activity metrics from pre-aggregated data...");

        // userAggregatedStats already has 'totalRatings' per user.
        // We need to rename 'totalRatings' to 'ratingsCount' for consistency if previous code expected that.
        Dataset<Row> userActivity = userAggregatedStats.select(
                functions.col("userId"),
                functions.col("totalRatings").as("ratingsCount") // Alias for consistency
        );

        Row userStatsOverall = userActivity.agg(
                functions.count("userId").as("totalUsers"),
                functions.avg("ratingsCount").as("avgRatingsPerUser"),
                functions.min("ratingsCount").as("minRatingsPerUser"),
                functions.max("ratingsCount").as("maxRatingsPerUser"),
                functions.expr("percentile_approx(ratingsCount, 0.5)").as("medianRatingsPerUser") // Use percentile_approx
        ).first();

        if (userStatsOverall != null) {
            metrics.addMetric("totalUsers", getLongFromRow(userStatsOverall, "totalUsers", 0L))
                    .addMetric("avgRatingsPerUser", getDoubleFromRow(userStatsOverall, "avgRatingsPerUser", 0.0))
                    .addMetric("minRatingsPerUser", getLongFromRow(userStatsOverall, "minRatingsPerUser", 0L))
                    .addMetric("maxRatingsPerUser", getLongFromRow(userStatsOverall, "maxRatingsPerUser", 0L))
                    .addMetric("medianRatingsPerUser", getDoubleFromRow(userStatsOverall, "medianRatingsPerUser", 0.0));
        }

        Dataset<Row> topUsers = userActivity.orderBy(functions.desc("ratingsCount")).limit(10);
        List<Row> collectedTopUsers = topUsers.collectAsList(); // Small, so collect is fine
        for(Row row : collectedTopUsers) {
            String userKey = "topUser_" + getIntFromRow(row, "userId", 0);
            metrics.addMetric(userKey + "_ratingsCount", getLongFromRow(row, "ratingsCount", 0L));
        }
        log.debug("Basic user activity metrics collected");
    }

    /**
     * Collects user engagement depth metrics from pre-aggregated data.
     */
    private void collectUserEngagementMetrics(Dataset<Row> userAggregatedStats, AnalyticsMetrics metrics) {
        log.debug("Collecting user engagement metrics from pre-aggregated data...");
        // userAggregatedStats already contains: totalRatings, avgRating, ratingStdDev (as ratingStdDev), uniqueMovies, engagementSpan

        Row engagementStatsOverall = userAggregatedStats.agg(
                // These are averages of per-user averages/counts, which is what the original code did
                functions.avg("totalRatings").as("avgTotalRatingsPerUser"), // This is same as avgRatingsPerUser
                functions.avg("avgRating").as("avgAvgUserRating"),
                functions.avg("ratingStdDev").as("avgRatingStdDev"), // Renamed from ratingVariance
                functions.avg("uniqueMovies").as("avgUniqueMoviesPerUser"),
                functions.avg("engagementSpan").as("avgEngagementSpan")
        ).first();

        if (engagementStatsOverall != null) {
            metrics.addMetric("engagement_avgRatingsPerUser", getDoubleFromRow(engagementStatsOverall, "avgTotalRatingsPerUser", 0.0))
                    .addMetric("engagement_avgUserRating", getDoubleFromRow(engagementStatsOverall, "avgAvgUserRating", 0.0))
                    .addMetricWithDefault("engagement_avgRatingStdDev", engagementStatsOverall.get(engagementStatsOverall.fieldIndex("avgRatingStdDev")), 0.0)
                    .addMetric("engagement_avgUniqueMoviesPerUser", getDoubleFromRow(engagementStatsOverall, "avgUniqueMoviesPerUser", 0.0))
                    .addMetric("engagement_avgEngagementSpanSeconds", getDoubleFromRow(engagementStatsOverall, "avgEngagementSpan", 0.0));
        }

        long highlyEngagedUsers = userAggregatedStats.filter(functions.col("totalRatings").gt(50)).count();
        long moderatelyEngagedUsers = userAggregatedStats.filter(functions.col("totalRatings").between(10, 50)).count();
        long lowEngagedUsers = userAggregatedStats.filter(functions.col("totalRatings").lt(10)).count();

        metrics.addMetric("highlyEngagedUsers", highlyEngagedUsers)
                .addMetric("moderatelyEngagedUsers", moderatelyEngagedUsers)
                .addMetric("lowEngagedUsers", lowEngagedUsers);
        log.debug("User engagement metrics collected");
    }

    /**
     * Collects user segmentation metrics from pre-aggregated data.
     */
    private void collectUserSegmentationMetrics(Dataset<Row> userAggregatedStats, AnalyticsMetrics metrics) {
        log.debug("Collecting user segmentation metrics from pre-aggregated data...");
        // userAggregatedStats contains: userId, totalRatings, avgRating, ratingStdDev, uniqueMovies

        Dataset<Row> userSegments = userAggregatedStats // Already grouped by user
                .withColumn("activityLevel",
                        functions.when(functions.col("totalRatings").gt(100), "High")
                                .when(functions.col("totalRatings").gt(20), "Medium")
                                .otherwise("Low"))
                .withColumn("ratingPattern",
                        functions.when(functions.col("avgRating").gt(4.0), "Positive")
                                .when(functions.col("avgRating").gt(2.5), "Neutral")
                                .otherwise("Critical"));

        Dataset<Row> segmentCounts = userSegments
                .groupBy("activityLevel", "ratingPattern")
                .count()
                .orderBy("activityLevel", "ratingPattern"); // Small result

        List<Row> collectedSegmentCounts = segmentCounts.collectAsList();
        for(Row row : collectedSegmentCounts) {
            String activityLevel = row.getString(row.fieldIndex("activityLevel"));
            String ratingPattern = row.getString(row.fieldIndex("ratingPattern"));
            String segmentKey = "segment_" + activityLevel + "_" + ratingPattern;
            metrics.addMetric(segmentKey, getLongFromRow(row, "count", 0L));
        }

        long totalUsers = userAggregatedStats.count(); // Count of unique users
        long highActivityUsers = userSegments.filter(functions.col("activityLevel").equalTo("High")).count();
        long mediumActivityUsers = userSegments.filter(functions.col("activityLevel").equalTo("Medium")).count();
        long lowActivityUsers = userSegments.filter(functions.col("activityLevel").equalTo("Low")).count();

        metrics.addMetric("segmentation_totalUsers", totalUsers);
        if (totalUsers > 0) {
            metrics.addMetric("segmentation_highActivityUsers", highActivityUsers)
                    .addMetric("segmentation_mediumActivityUsers", mediumActivityUsers)
                    .addMetric("segmentation_lowActivityUsers", lowActivityUsers)
                    .addMetric("segmentation_highActivityPercentage", (double) highActivityUsers / totalUsers * 100.0);
        } else {
            metrics.addMetric("segmentation_highActivityUsers", 0L)
                    .addMetric("segmentation_mediumActivityUsers", 0L)
                    .addMetric("segmentation_lowActivityUsers", 0L)
                    .addMetric("segmentation_highActivityPercentage", 0.0);
        }
        log.debug("User segmentation metrics collected");
    }

    @Override
    public String getAnalyticsType() {
        // This service combines multiple aspects. USER_ACTIVITY is the Enum name used in the original.
        return AnalyticsType.USER_ACTIVITY.name();
    }

    @Override
    public boolean canProcess(Dataset<Row> ratingsDf, Dataset<Row> moviesData, Dataset<Row> tagsData) {
        // Requires ratingsDf with specific columns for user-level aggregations.
        if (ratingsDf == null || ratingsDf.isEmpty()) return false;
        List<String> requiredColumns = java.util.Arrays.asList("userId", "movieId", "ratingActual", "timestampEpochSeconds");
        List<String> actualColumns = java.util.Arrays.asList(ratingsDf.columns());
        return new HashSet<>(actualColumns).containsAll(requiredColumns);
    }

    @Override
    public int getPriority() {
        return 10;
    }
}
