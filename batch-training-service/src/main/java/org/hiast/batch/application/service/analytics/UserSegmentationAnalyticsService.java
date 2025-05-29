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
 * Service responsible for collecting user segmentation analytics.
 * Analyzes user behavior patterns to segment users into meaningful groups
 * based on activity levels and rating patterns.
 * <p>
 * This service generates a separate analytics document focused specifically
 * on user segmentation metrics, exactly as in the original implementation.
 */
public class UserSegmentationAnalyticsService implements AnalyticsCollector {

    private static final Logger log = LoggerFactory.getLogger(UserSegmentationAnalyticsService.class);

    @Override
    public List<DataAnalytics> collectAnalytics(Dataset<Row> ratingsDf,
                                                Dataset<Row> moviesData,
                                                Dataset<Row> tagsData,
                                                ALSTrainingPipelineContext context) {

        if (!canProcess(ratingsDf, moviesData, tagsData)) {
            throw new AnalyticsCollectionException("USER_SEGMENTATION", "Insufficient data for user segmentation analytics");
        }

        try {
            log.info("Collecting user segmentation analytics...");

            AnalyticsMetrics metrics = AnalyticsMetrics.builder();

            // Collect user segmentation metrics - exact same as original
            collectUserSegmentationMetrics(ratingsDf, metrics);

            log.info("User segmentation analytics collection completed with {} metrics", metrics.size());

            return Collections.singletonList(new DataAnalytics(
                    "user_segmentation_" + UUID.randomUUID().toString().substring(0, 8),
                    Instant.now(),
                    AnalyticsType.USER_SEGMENTATION,
                    metrics.build(),
                    "User segmentation and behavior pattern analytics"
            ));

        } catch (Exception e) {
            log.error("Error collecting user segmentation analytics: {}", e.getMessage(), e);
            throw new AnalyticsCollectionException("USER_SEGMENTATION", e.getMessage(), e);
        }
    }

    /**
     * Collects user segmentation metrics - exact same as original implementation.
     */
    private void collectUserSegmentationMetrics(Dataset<Row> ratingsDf, AnalyticsMetrics metrics) {
        log.info("Collecting user segmentation analytics...");

        try {
            // Calculate user behavior metrics for segmentation - exact same as original
            Dataset<Row> userBehavior = ratingsDf.groupBy("userId")
                    .agg(
                            functions.count("ratingActual").as("totalRatings"),
                            functions.avg("ratingActual").as("avgRating"),
                            functions.stddev("ratingActual").as("ratingStdDev"),
                            functions.countDistinct("movieId").as("uniqueMovies")
                    );

            // Define user segments based on activity and rating patterns - exact same as original
            Dataset<Row> userSegments = userBehavior
                    .withColumn("activityLevel",
                            functions.when(functions.col("totalRatings").gt(100), "High")
                                    .when(functions.col("totalRatings").gt(20), "Medium")
                                    .otherwise("Low"))
                    .withColumn("ratingPattern",
                            functions.when(functions.col("avgRating").gt(4.0), "Positive")
                                    .when(functions.col("avgRating").gt(2.5), "Neutral")
                                    .otherwise("Critical"));

            // Count users in each segment - exact same as original
            Dataset<Row> segmentCounts = userSegments
                    .groupBy("activityLevel", "ratingPattern")
                    .count()
                    .orderBy("activityLevel", "ratingPattern");

            // Add segment counts - exact same as original
            segmentCounts.collectAsList().forEach(row -> {
                String activityLevel = row.getString(0);
                String ratingPattern = row.getString(1);
                long count = row.getLong(2);
                String segmentKey = "segment_" + activityLevel + "_" + ratingPattern;
                metrics.addMetric(segmentKey, count);
            });

            // Calculate overall segment statistics - exact same as original
            long totalUsers = userBehavior.count();
            long highActivityUsers = userSegments.filter(functions.col("activityLevel").equalTo("High")).count();
            long mediumActivityUsers = userSegments.filter(functions.col("activityLevel").equalTo("Medium")).count();
            long lowActivityUsers = userSegments.filter(functions.col("activityLevel").equalTo("Low")).count();

            metrics.addMetric("totalUsers", totalUsers)
                    .addMetric("highActivityUsers", highActivityUsers)
                    .addMetric("mediumActivityUsers", mediumActivityUsers)
                    .addMetric("lowActivityUsers", lowActivityUsers)
                    .addMetric("highActivityPercentage", (double) highActivityUsers / totalUsers * 100)
                    .addMetric("mediumActivityPercentage", (double) mediumActivityUsers / totalUsers * 100)
                    .addMetric("lowActivityPercentage", (double) lowActivityUsers / totalUsers * 100);

            // Calculate rating pattern distribution - exact same as original
            long positiveUsers = userSegments.filter(functions.col("ratingPattern").equalTo("Positive")).count();
            long neutralUsers = userSegments.filter(functions.col("ratingPattern").equalTo("Neutral")).count();
            long criticalUsers = userSegments.filter(functions.col("ratingPattern").equalTo("Critical")).count();

            metrics.addMetric("positiveUsers", positiveUsers)
                    .addMetric("neutralUsers", neutralUsers)
                    .addMetric("criticalUsers", criticalUsers)
                    .addMetric("positiveUsersPercentage", (double) positiveUsers / totalUsers * 100)
                    .addMetric("neutralUsersPercentage", (double) neutralUsers / totalUsers * 100)
                    .addMetric("criticalUsersPercentage", (double) criticalUsers / totalUsers * 100);

            // Advanced segmentation analysis - exact same as original
            Dataset<Row> advancedSegments = userBehavior
                    .withColumn("engagementLevel",
                            functions.when(functions.col("uniqueMovies").gt(functions.col("totalRatings").multiply(0.8)), "Explorer")
                                    .when(functions.col("uniqueMovies").gt(functions.col("totalRatings").multiply(0.5)), "Selective")
                                    .otherwise("Focused"))
                    .withColumn("ratingConsistency",
                            functions.when(functions.col("ratingStdDev").lt(0.5), "Consistent")
                                    .when(functions.col("ratingStdDev").lt(1.0), "Moderate")
                                    .otherwise("Variable"));

            // Count advanced segments - exact same as original
            Dataset<Row> advancedSegmentCounts = advancedSegments
                    .groupBy("engagementLevel", "ratingConsistency")
                    .count()
                    .orderBy("engagementLevel", "ratingConsistency");

            // Add advanced segment counts - exact same as original
            advancedSegmentCounts.collectAsList().forEach(row -> {
                String engagementLevel = row.getString(0);
                String ratingConsistency = row.getString(1);
                long count = row.getLong(2);
                String segmentKey = "advancedSegment_" + engagementLevel + "_" + ratingConsistency;
                metrics.addMetric(segmentKey, count);
            });

            // Calculate segment quality metrics - exact same as original
            Row segmentQuality = userBehavior.agg(
                    functions.avg("totalRatings").as("avgRatingsAcrossUsers"),
                    functions.avg("avgRating").as("avgRatingAcrossUsers"),
                    functions.avg("uniqueMovies").as("avgUniqueMoviesAcrossUsers"),
                    functions.stddev("totalRatings").as("activityVariation"),
                    functions.stddev("avgRating").as("ratingVariation")
            ).first();

            metrics.addMetric("avgRatingsAcrossUsers", segmentQuality.getDouble(0))
                    .addMetric("avgRatingAcrossUsers", segmentQuality.getDouble(1))
                    .addMetric("avgUniqueMoviesAcrossUsers", segmentQuality.getDouble(2))
                    .addMetricWithDefault("activityVariation", segmentQuality.get(3), 0.0)
                    .addMetricWithDefault("ratingVariation", segmentQuality.get(4), 0.0);

            // Calculate segmentation effectiveness score - exact same as original
            double segmentationScore = 100.0;

            // Penalize if segments are too unbalanced
            double maxSegmentPercentage = Math.max(Math.max(
                            (double) highActivityUsers / totalUsers,
                            (double) mediumActivityUsers / totalUsers),
                    (double) lowActivityUsers / totalUsers) * 100;

            if (maxSegmentPercentage > 80) segmentationScore -= 30;
            else if (maxSegmentPercentage > 70) segmentationScore -= 20;
            else if (maxSegmentPercentage > 60) segmentationScore -= 10;

            metrics.addMetric("segmentationEffectivenessScore", segmentationScore)
                    .addMetric("maxSegmentPercentage", maxSegmentPercentage);

        } catch (Exception e) {
            log.error("Error collecting user segmentation analytics: {}", e.getMessage(), e);
            throw new AnalyticsCollectionException("USER_SEGMENTATION", e.getMessage(), e);
        }
    }

    @Override
    public String getAnalyticsType() {
        return "USER_SEGMENTATION";
    }

    @Override
    public boolean canProcess(Dataset<Row> ratingsDf, Dataset<Row> moviesData, Dataset<Row> tagsData) {
        return ratingsDf != null && !ratingsDf.isEmpty();
    }

    @Override
    public int getPriority() {
        return 35; // Medium priority for user segmentation
    }
}
