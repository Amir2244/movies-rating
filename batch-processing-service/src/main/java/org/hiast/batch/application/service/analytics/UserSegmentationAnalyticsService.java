package org.hiast.batch.application.service.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.storage.StorageLevel;
// import org.hiast.batch.application.pipeline.BasePipelineContext; // Not used in the provided method signature
import org.hiast.batch.domain.exception.AnalyticsCollectionException;
import org.hiast.batch.domain.model.AnalyticsType;
import org.hiast.batch.domain.model.DataAnalytics;
import org.hiast.batch.domain.model.analytics.AnalyticsMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;

/**
 * Service responsible for collecting user segmentation analytics.
 * Analyzes user behavior patterns to segment users into meaningful groups
 * based on activity levels and rating patterns.
 * <p>
 * This service generates a separate analytics document focused specifically
 * on user segmentation metrics.
 * OPTIMIZED VERSION v3: Combines totalUsers count with other segment counts.
 */
public class UserSegmentationAnalyticsService implements AnalyticsCollector {

    private static final Logger log = LoggerFactory.getLogger(UserSegmentationAnalyticsService.class);

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
            throw new AnalyticsCollectionException("USER_SEGMENTATION", "Insufficient data for user segmentation analytics");
        }
        List<DataAnalytics> collectedAnalytics = new ArrayList<>();

        try {
            log.info("Collecting user segmentation analytics (Optimized v3)...");
            AnalyticsMetrics metrics = AnalyticsMetrics.builder();

            collectUserSegmentationMetricsInternal(ratingsDf, metrics);

            log.info("User segmentation analytics collection completed with {} metrics", metrics.size());

            collectedAnalytics.add(new DataAnalytics(
                    "user_segmentation_" + UUID.randomUUID().toString().substring(0, 8),
                    Instant.now(),
                    AnalyticsType.USER_SEGMENTATION,
                    metrics.build(),
                    "User segmentation and behavior pattern analytics"
            ));
            return collectedAnalytics;

        } catch (Exception e) {
            log.error("Error collecting user segmentation analytics: {}", e.getMessage(), e);
            throw new AnalyticsCollectionException("USER_SEGMENTATION", e.getMessage(), e);
        }
    }

    /**
     * Internal method to collect user segmentation metrics.
     */
    private void collectUserSegmentationMetricsInternal(Dataset<Row> ratingsDf, AnalyticsMetrics metrics) {
        log.debug("Executing internal user segmentation metrics collection (Optimized v3)...");

        Dataset<Row> relevantRatings = ratingsDf.select(
                functions.col("userId"),
                functions.col("movieId"),
                functions.col("ratingActual")
        );

        Dataset<Row> userBehavior = relevantRatings.groupBy("userId")
                .agg(
                        functions.count("ratingActual").as("totalRatings"),
                        functions.avg("ratingActual").as("avgRating"),
                        functions.stddev_samp("ratingActual").as("ratingStdDev"),
                        functions.countDistinct("movieId").as("uniqueMovies")
                );

        userBehavior.persist(StorageLevel.MEMORY_AND_DISK_SER());
        // Note: totalUsers will be derived more efficiently below, but counting userBehavior
        // here helps to materialize it and log its size if needed for debugging.
        long initialUserBehaviorCount = userBehavior.count();
        log.debug("Persisted userBehavior dataset. Initial count of unique users: {}", initialUserBehaviorCount);


        Dataset<Row> userSegments = userBehavior
                .withColumn("activityLevel",
                        functions.when(functions.col("totalRatings").gt(100), "High")
                                .when(functions.col("totalRatings").gt(20), "Medium")
                                .otherwise("Low"))
                .withColumn("ratingPattern",
                        functions.when(functions.col("avgRating").gt(4.0), "Positive")
                                .when(functions.col("avgRating").gt(2.5), "Neutral")
                                .otherwise("Critical"));

        userSegments.persist(StorageLevel.MEMORY_AND_DISK_SER());
        log.debug("Persisted userSegments dataset. First few rows:");
        userSegments.show(5, false);


        // --- OPTIMIZATION V3: Combine totalUsers count with other segment counts ---
        Row combinedCounts = userSegments.agg(
                functions.count("*").as("totalUsers"), // Total users from userSegments
                functions.sum(functions.when(functions.col("activityLevel").equalTo("High"), 1).otherwise(0)).as("highActivityUsers"),
                functions.sum(functions.when(functions.col("activityLevel").equalTo("Medium"), 1).otherwise(0)).as("mediumActivityUsers"),
                functions.sum(functions.when(functions.col("activityLevel").equalTo("Low"), 1).otherwise(0)).as("lowActivityUsers"),
                functions.sum(functions.when(functions.col("ratingPattern").equalTo("Positive"), 1).otherwise(0)).as("positiveUsers"),
                functions.sum(functions.when(functions.col("ratingPattern").equalTo("Neutral"), 1).otherwise(0)).as("neutralUsers"),
                functions.sum(functions.when(functions.col("ratingPattern").equalTo("Critical"), 1).otherwise(0)).as("criticalUsers")
        ).first();

        long totalUsers = 0L;
        long highActivityUsers = 0L, mediumActivityUsers = 0L, lowActivityUsers = 0L;
        long positiveUsers = 0L, neutralUsers = 0L, criticalUsers = 0L;

        if (combinedCounts != null) {
            totalUsers = getLongFromRow(combinedCounts, "totalUsers", 0L);
            highActivityUsers = getLongFromRow(combinedCounts, "highActivityUsers", 0L);
            mediumActivityUsers = getLongFromRow(combinedCounts, "mediumActivityUsers", 0L);
            lowActivityUsers = getLongFromRow(combinedCounts, "lowActivityUsers", 0L);
            positiveUsers = getLongFromRow(combinedCounts, "positiveUsers", 0L);
            neutralUsers = getLongFromRow(combinedCounts, "neutralUsers", 0L);
            criticalUsers = getLongFromRow(combinedCounts, "criticalUsers", 0L);
        }

        metrics.addMetric("totalUsers", totalUsers);

        if (totalUsers > 0) {
            metrics.addMetric("highActivityUsers", highActivityUsers)
                    .addMetric("mediumActivityUsers", mediumActivityUsers)
                    .addMetric("lowActivityUsers", lowActivityUsers)
                    .addMetric("highActivityPercentage", (double) highActivityUsers / totalUsers * 100.0)
                    .addMetric("mediumActivityPercentage", (double) mediumActivityUsers / totalUsers * 100.0)
                    .addMetric("lowActivityPercentage", (double) lowActivityUsers / totalUsers * 100.0);

            metrics.addMetric("positiveUsers", positiveUsers)
                    .addMetric("neutralUsers", neutralUsers)
                    .addMetric("criticalUsers", criticalUsers)
                    .addMetric("positiveUsersPercentage", (double) positiveUsers / totalUsers * 100.0)
                    .addMetric("neutralUsersPercentage", (double) neutralUsers / totalUsers * 100.0)
                    .addMetric("criticalUsersPercentage", (double) criticalUsers / totalUsers * 100.0);
        } else {
            // Add default zero values if totalUsers is 0
            metrics.addMetric("highActivityUsers", 0L)
                    .addMetric("mediumActivityUsers", 0L)
                    .addMetric("lowActivityUsers", 0L)
                    .addMetric("highActivityPercentage", 0.0)
                    .addMetric("mediumActivityPercentage", 0.0)
                    .addMetric("lowActivityPercentage", 0.0);
            metrics.addMetric("positiveUsers", 0L)
                    .addMetric("neutralUsers", 0L)
                    .addMetric("criticalUsers", 0L)
                    .addMetric("positiveUsersPercentage", 0.0)
                    .addMetric("neutralUsersPercentage", 0.0)
                    .addMetric("criticalUsersPercentage", 0.0);
        }

        Dataset<Row> segmentCounts = userSegments // Reuses persisted userSegments
                .groupBy("activityLevel", "ratingPattern")
                .count()
                .orderBy("activityLevel", "ratingPattern");

        List<Row> collectedSegmentCounts = segmentCounts.collectAsList();
        for(Row row : collectedSegmentCounts) {
            String activityLevel = row.getString(row.fieldIndex("activityLevel"));
            String ratingPattern = row.getString(row.fieldIndex("ratingPattern"));
            long count = getLongFromRow(row, "count", 0L);
            String segmentKey = "segment_" + activityLevel + "_" + ratingPattern;
            metrics.addMetric(segmentKey, count);
        }

        Dataset<Row> advancedSegments = userBehavior // Reuses persisted userBehavior
                .withColumn("engagementLevel",
                        functions.when(functions.col("uniqueMovies").gt(functions.col("totalRatings").multiply(0.8)), "Explorer")
                                .when(functions.col("uniqueMovies").gt(functions.col("totalRatings").multiply(0.5)), "Selective")
                                .otherwise("Focused"))
                .withColumn("ratingConsistency",
                        functions.when(functions.col("ratingStdDev").isNotNull().and(functions.col("ratingStdDev").lt(0.5)), "Consistent")
                                .when(functions.col("ratingStdDev").isNotNull().and(functions.col("ratingStdDev").lt(1.0)), "Moderate")
                                .otherwise("Variable"));

        Dataset<Row> advancedSegmentCounts = advancedSegments
                .groupBy("engagementLevel", "ratingConsistency")
                .count()
                .orderBy("engagementLevel", "ratingConsistency");

        List<Row> collectedAdvancedSegmentCounts = advancedSegmentCounts.collectAsList();
        for(Row row : collectedAdvancedSegmentCounts) {
            String engagementLevel = row.getString(row.fieldIndex("engagementLevel"));
            String ratingConsistency = row.getString(row.fieldIndex("ratingConsistency"));
            long count = getLongFromRow(row, "count", 0L);
            String segmentKey = "advancedSegment_" + engagementLevel + "_" + ratingConsistency;
            metrics.addMetric(segmentKey, count);
        }

        Row segmentQuality = userBehavior.agg( // Reuses persisted userBehavior
                functions.avg("totalRatings").as("avgRatingsAcrossUsers"),
                functions.avg("avgRating").as("avgRatingAcrossUsers"),
                functions.avg("uniqueMovies").as("avgUniqueMoviesAcrossUsers"),
                functions.stddev_samp("totalRatings").as("activityVariation"),
                functions.stddev_samp("avgRating").as("ratingVariation")
        ).first();

        if (segmentQuality != null) {
            metrics.addMetric("avgRatingsAcrossUsers", getDoubleFromRow(segmentQuality, "avgRatingsAcrossUsers", 0.0))
                    .addMetric("avgRatingAcrossUsers", getDoubleFromRow(segmentQuality, "avgRatingAcrossUsers", 0.0))
                    .addMetric("avgUniqueMoviesAcrossUsers", getDoubleFromRow(segmentQuality, "avgUniqueMoviesAcrossUsers", 0.0))
                    .addMetricWithDefault("activityVariation", segmentQuality.get(segmentQuality.fieldIndex("activityVariation")), 0.0)
                    .addMetricWithDefault("ratingVariation", segmentQuality.get(segmentQuality.fieldIndex("ratingVariation")), 0.0);
        } else {
            metrics.addMetric("avgRatingsAcrossUsers", 0.0)
                    .addMetric("avgRatingAcrossUsers", 0.0)
                    .addMetric("avgUniqueMoviesAcrossUsers", 0.0)
                    .addMetric("activityVariation", 0.0)
                    .addMetric("ratingVariation", 0.0);
        }

        double segmentationScore = 100.0;
        if (totalUsers > 0) { // Use the efficiently calculated totalUsers
            double maxSegmentPercentage = Math.max(Math.max(
                            (double) highActivityUsers / totalUsers, // Use Java variables
                            (double) mediumActivityUsers / totalUsers),
                    (double) lowActivityUsers / totalUsers) * 100.0;

            if (maxSegmentPercentage > 80) segmentationScore -= 30;
            else if (maxSegmentPercentage > 70) segmentationScore -= 20;
            else if (maxSegmentPercentage > 60) segmentationScore -= 10;
            metrics.addMetric("maxSegmentPercentage", maxSegmentPercentage);
        } else {
            metrics.addMetric("maxSegmentPercentage", 0.0);
        }
        metrics.addMetric("segmentationEffectivenessScore", segmentationScore);

        userSegments.unpersist();
        userBehavior.unpersist();
        log.debug("Unpersisted userBehavior and userSegments datasets.");
    }

    @Override
    public String getAnalyticsType() {
        return "USER_SEGMENTATION";
    }

    @Override
    public boolean canProcess(Dataset<Row> ratingsDf, Dataset<Row> moviesData, Dataset<Row> tagsData) {
        if (ratingsDf == null || ratingsDf.isEmpty()) return false;
        List<String> requiredColumns = java.util.Arrays.asList("userId", "movieId", "ratingActual");
        List<String> actualColumns = java.util.Arrays.asList(ratingsDf.columns());
        return new HashSet<>(actualColumns).containsAll(requiredColumns);
    }

    @Override
    public int getPriority() {
        return 35;
    }
}
