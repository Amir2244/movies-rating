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
 * Service responsible for collecting user-related analytics.
 * Handles user activity, engagement, and segmentation analytics.
 * 
 * This service follows the Single Responsibility Principle by focusing
 * only on user-related analytics collection.
 */
public class UserAnalyticsService implements AnalyticsCollector {
    
    private static final Logger log = LoggerFactory.getLogger(UserAnalyticsService.class);
    
    @Override
    public DataAnalytics collectAnalytics(Dataset<Row> ratingsDf, 
                                        Dataset<Row> moviesData, 
                                        Dataset<Row> tagsData, 
                                        ALSTrainingPipelineContext context) {
        
        if (!canProcess(ratingsDf, moviesData, tagsData)) {
            throw new AnalyticsCollectionException("USER_ACTIVITY", "Insufficient data for user analytics");
        }
        
        try {
            log.info("Collecting user activity analytics...");
            
            AnalyticsMetrics metrics = AnalyticsMetrics.builder();
            
            // Collect basic user activity metrics
            collectUserActivityMetrics(ratingsDf, metrics);
            
            // Collect user engagement metrics
            collectUserEngagementMetrics(ratingsDf, metrics);
            
            // Collect user segmentation metrics
            collectUserSegmentationMetrics(ratingsDf, metrics);
            
            log.info("User analytics collection completed with {} metrics", metrics.size());
            
            return new DataAnalytics(
                    "user_activity_" + UUID.randomUUID().toString().substring(0, 8),
                    Instant.now(),
                    AnalyticsType.USER_ACTIVITY,
                    metrics.build(),
                    "Comprehensive user activity, engagement, and segmentation analytics"
            );
            
        } catch (Exception e) {
            log.error("Error collecting user analytics: {}", e.getMessage(), e);
            throw new AnalyticsCollectionException("USER_ACTIVITY", e.getMessage(), e);
        }
    }
    
    /**
     * Collects basic user activity metrics.
     */
    private void collectUserActivityMetrics(Dataset<Row> ratingsDf, AnalyticsMetrics metrics) {
        log.debug("Collecting basic user activity metrics...");
        
        // Calculate user activity statistics
        Dataset<Row> userActivity = ratingsDf.groupBy("userId")
                .count()
                .withColumnRenamed("count", "ratingsCount");

        Row userStats = userActivity.agg(
                functions.count("userId").as("totalUsers"),
                functions.avg("ratingsCount").as("avgRatingsPerUser"),
                functions.min("ratingsCount").as("minRatingsPerUser"),
                functions.max("ratingsCount").as("maxRatingsPerUser"),
                functions.expr("percentile(ratingsCount, 0.5)").as("medianRatingsPerUser")
        ).first();

        metrics.addMetric("totalUsers", userStats.getLong(0))
               .addMetric("avgRatingsPerUser", userStats.getDouble(1))
               .addMetric("minRatingsPerUser", userStats.getLong(2))
               .addMetric("maxRatingsPerUser", userStats.getLong(3))
               .addMetric("medianRatingsPerUser", userStats.getDouble(4));

        // Get top 10 most active users
        Dataset<Row> topUsers = userActivity.orderBy(functions.desc("ratingsCount")).limit(10);
        
        topUsers.collectAsList().forEach(row -> {
            String userKey = "topUser_" + row.getInt(0);
            metrics.addMetric(userKey + "_ratingsCount", row.getLong(1));
        });
        
        log.debug("Basic user activity metrics collected");
    }
    
    /**
     * Collects user engagement depth metrics.
     */
    private void collectUserEngagementMetrics(Dataset<Row> ratingsDf, AnalyticsMetrics metrics) {
        log.debug("Collecting user engagement metrics...");
        
        // Calculate engagement metrics
        Dataset<Row> userEngagement = ratingsDf.groupBy("userId")
                .agg(
                        functions.count("ratingActual").as("totalRatings"),
                        functions.avg("ratingActual").as("avgRating"),
                        functions.stddev("ratingActual").as("ratingVariance"),
                        functions.countDistinct("movieId").as("uniqueMovies"),
                        functions.max("timestampEpochSeconds").minus(functions.min("timestampEpochSeconds")).as("engagementSpan")
                );

        // Calculate engagement statistics
        Row engagementStats = userEngagement.agg(
                functions.avg("totalRatings").as("avgRatingsPerUser"),
                functions.avg("avgRating").as("avgUserRating"),
                functions.avg("ratingVariance").as("avgRatingVariance"),
                functions.avg("uniqueMovies").as("avgUniqueMoviesPerUser"),
                functions.avg("engagementSpan").as("avgEngagementSpan")
        ).first();

        metrics.addMetric("engagement_avgRatingsPerUser", engagementStats.getDouble(0))
               .addMetric("engagement_avgUserRating", engagementStats.getDouble(1))
               .addMetricWithDefault("engagement_avgRatingVariance", engagementStats.get(2), 0.0)
               .addMetric("engagement_avgUniqueMoviesPerUser", engagementStats.getDouble(3))
               .addMetric("engagement_avgEngagementSpanSeconds", engagementStats.getDouble(4));

        // Calculate engagement levels
        long highlyEngagedUsers = userEngagement.filter(functions.col("totalRatings").gt(50)).count();
        long moderatelyEngagedUsers = userEngagement.filter(functions.col("totalRatings").between(10, 50)).count();
        long lowEngagedUsers = userEngagement.filter(functions.col("totalRatings").lt(10)).count();

        metrics.addMetric("highlyEngagedUsers", highlyEngagedUsers)
               .addMetric("moderatelyEngagedUsers", moderatelyEngagedUsers)
               .addMetric("lowEngagedUsers", lowEngagedUsers);
        
        log.debug("User engagement metrics collected");
    }
    
    /**
     * Collects user segmentation metrics.
     */
    private void collectUserSegmentationMetrics(Dataset<Row> ratingsDf, AnalyticsMetrics metrics) {
        log.debug("Collecting user segmentation metrics...");
        
        // Calculate user behavior metrics for segmentation
        Dataset<Row> userBehavior = ratingsDf.groupBy("userId")
                .agg(
                        functions.count("ratingActual").as("totalRatings"),
                        functions.avg("ratingActual").as("avgRating"),
                        functions.stddev("ratingActual").as("ratingStdDev"),
                        functions.countDistinct("movieId").as("uniqueMovies")
                );

        // Define user segments based on activity and rating patterns
        Dataset<Row> userSegments = userBehavior
                .withColumn("activityLevel",
                        functions.when(functions.col("totalRatings").gt(100), "High")
                                .when(functions.col("totalRatings").gt(20), "Medium")
                                .otherwise("Low"))
                .withColumn("ratingPattern",
                        functions.when(functions.col("avgRating").gt(4.0), "Positive")
                                .when(functions.col("avgRating").gt(2.5), "Neutral")
                                .otherwise("Critical"));

        // Count users in each segment
        Dataset<Row> segmentCounts = userSegments
                .groupBy("activityLevel", "ratingPattern")
                .count()
                .orderBy("activityLevel", "ratingPattern");

        // Add segment counts
        segmentCounts.collectAsList().forEach(row -> {
            String segmentKey = "segment_" + row.getString(0) + "_" + row.getString(1);
            metrics.addMetric(segmentKey, row.getLong(2));
        });

        // Calculate overall segment statistics
        long totalUsers = userBehavior.count();
        long highActivityUsers = userSegments.filter(functions.col("activityLevel").equalTo("High")).count();
        long mediumActivityUsers = userSegments.filter(functions.col("activityLevel").equalTo("Medium")).count();
        long lowActivityUsers = userSegments.filter(functions.col("activityLevel").equalTo("Low")).count();

        metrics.addMetric("segmentation_totalUsers", totalUsers)
               .addMetric("segmentation_highActivityUsers", highActivityUsers)
               .addMetric("segmentation_mediumActivityUsers", mediumActivityUsers)
               .addMetric("segmentation_lowActivityUsers", lowActivityUsers)
               .addMetric("segmentation_highActivityPercentage", (double) highActivityUsers / totalUsers * 100);
        
        log.debug("User segmentation metrics collected");
    }
    
    @Override
    public String getAnalyticsType() {
        return "USER_ANALYTICS";
    }
    
    @Override
    public boolean canProcess(Dataset<Row> ratingsDf, Dataset<Row> moviesData, Dataset<Row> tagsData) {
        return ratingsDf != null && !ratingsDf.isEmpty();
    }
    
    @Override
    public int getPriority() {
        return 10; // High priority for user analytics
    }
}
