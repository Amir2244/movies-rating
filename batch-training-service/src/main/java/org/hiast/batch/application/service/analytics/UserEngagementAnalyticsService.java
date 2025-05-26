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
 * Service responsible for collecting user engagement analytics.
 * Analyzes user engagement depth, rating variance, and interaction patterns
 * to understand how deeply users engage with the platform.
 * 
 * This service generates a separate analytics document focused specifically
 * on user engagement metrics, exactly as in the original implementation.
 */
public class UserEngagementAnalyticsService implements AnalyticsCollector {
    
    private static final Logger log = LoggerFactory.getLogger(UserEngagementAnalyticsService.class);
    
    @Override
    public List<DataAnalytics> collectAnalytics(Dataset<Row> ratingsDf,
                                                Dataset<Row> moviesData,
                                                Dataset<Row> tagsData,
                                                ALSTrainingPipelineContext context) {
        
        if (!canProcess(ratingsDf, moviesData, tagsData)) {
            throw new AnalyticsCollectionException("USER_ENGAGEMENT", "Insufficient data for user engagement analytics");
        }
        
        try {
            log.info("Collecting user engagement analytics...");
            
            AnalyticsMetrics metrics = AnalyticsMetrics.builder();
            
            // Collect user engagement metrics - exact same as original
            collectUserEngagementMetrics(ratingsDf, metrics);
            
            log.info("User engagement analytics collection completed with {} metrics", metrics.size());
            
            return Collections.singletonList(new DataAnalytics(
                    "user_engagement_" + UUID.randomUUID().toString().substring(0, 8),
                    Instant.now(),
                    AnalyticsType.USER_ENGAGEMENT,
                    metrics.build(),
                    "User engagement depth and interaction pattern analytics"
            ));
            
        } catch (Exception e) {
            log.error("Error collecting user engagement analytics: {}", e.getMessage(), e);
            throw new AnalyticsCollectionException("USER_ENGAGEMENT", e.getMessage(), e);
        }
    }
    
    /**
     * Collects user engagement depth metrics - exact same as original implementation.
     */
    private void collectUserEngagementMetrics(Dataset<Row> ratingsDf, AnalyticsMetrics metrics) {
        log.info("Collecting user engagement analytics...");

        try {
            // Calculate engagement metrics - exact same as original
            Dataset<Row> userEngagement = ratingsDf.groupBy("userId")
                    .agg(
                            functions.count("ratingActual").as("totalRatings"),
                            functions.avg("ratingActual").as("avgRating"),
                            functions.stddev("ratingActual").as("ratingVariance"),
                            functions.countDistinct("movieId").as("uniqueMovies"),
                            functions.max("timestampEpochSeconds").minus(functions.min("timestampEpochSeconds")).as("engagementSpan")
                    );

            // Calculate engagement statistics - exact same as original
            Row engagementStats = userEngagement.agg(
                    functions.avg("totalRatings").as("avgRatingsPerUser"),
                    functions.avg("avgRating").as("avgUserRating"),
                    functions.avg("ratingVariance").as("avgRatingVariance"),
                    functions.avg("uniqueMovies").as("avgUniqueMoviesPerUser"),
                    functions.avg("engagementSpan").as("avgEngagementSpan")
            ).first();

            metrics.addMetric("avgRatingsPerUser", engagementStats.getDouble(0))
                   .addMetric("avgUserRating", engagementStats.getDouble(1))
                   .addMetricWithDefault("avgRatingVariance", engagementStats.get(2), 0.0)
                   .addMetric("avgUniqueMoviesPerUser", engagementStats.getDouble(3))
                   .addMetric("avgEngagementSpanSeconds", engagementStats.getDouble(4));

            // Calculate engagement levels - exact same as original
            long highlyEngagedUsers = userEngagement.filter(functions.col("totalRatings").gt(50)).count();
            long moderatelyEngagedUsers = userEngagement.filter(functions.col("totalRatings").between(10, 50)).count();
            long lowEngagedUsers = userEngagement.filter(functions.col("totalRatings").lt(10)).count();

            metrics.addMetric("highlyEngagedUsers", highlyEngagedUsers)
                   .addMetric("moderatelyEngagedUsers", moderatelyEngagedUsers)
                   .addMetric("lowEngagedUsers", lowEngagedUsers);

            // Calculate engagement diversity metrics - exact same as original
            long totalUsers = userEngagement.count();
            double highEngagementPercentage = (double) highlyEngagedUsers / totalUsers * 100;
            double moderateEngagementPercentage = (double) moderatelyEngagedUsers / totalUsers * 100;
            double lowEngagementPercentage = (double) lowEngagedUsers / totalUsers * 100;

            metrics.addMetric("totalEngagedUsers", totalUsers)
                   .addMetric("highEngagementPercentage", highEngagementPercentage)
                   .addMetric("moderateEngagementPercentage", moderateEngagementPercentage)
                   .addMetric("lowEngagementPercentage", lowEngagementPercentage);

            // Calculate rating behavior patterns - exact same as original
            Dataset<Row> ratingBehavior = userEngagement
                    .withColumn("ratingFrequency", 
                            functions.when(functions.col("totalRatings").gt(100), "VeryActive")
                                    .when(functions.col("totalRatings").gt(50), "Active")
                                    .when(functions.col("totalRatings").gt(10), "Moderate")
                                    .otherwise("Casual"))
                    .withColumn("ratingDiversity",
                            functions.when(functions.col("uniqueMovies").gt(functions.col("totalRatings").multiply(0.8)), "HighDiversity")
                                    .when(functions.col("uniqueMovies").gt(functions.col("totalRatings").multiply(0.5)), "ModerateDiversity")
                                    .otherwise("LowDiversity"));

            // Count users by behavior patterns - exact same as original
            Dataset<Row> behaviorCounts = ratingBehavior
                    .groupBy("ratingFrequency", "ratingDiversity")
                    .count()
                    .orderBy("ratingFrequency", "ratingDiversity");

            // Add behavior pattern counts - exact same as original
            behaviorCounts.collectAsList().forEach(row -> {
                String frequency = row.getString(0);
                String diversity = row.getString(1);
                long count = row.getLong(2);
                String behaviorKey = "behavior_" + frequency + "_" + diversity;
                metrics.addMetric(behaviorKey, count);
            });

            // Calculate engagement quality metrics - exact same as original
            Row qualityStats = userEngagement.agg(
                    functions.expr("percentile(totalRatings, 0.5)").as("medianRatingsPerUser"),
                    functions.expr("percentile(uniqueMovies, 0.5)").as("medianUniqueMoviesPerUser"),
                    functions.max("totalRatings").as("maxRatingsPerUser"),
                    functions.max("uniqueMovies").as("maxUniqueMoviesPerUser")
            ).first();

            metrics.addMetric("medianRatingsPerUser", qualityStats.getDouble(0))
                   .addMetric("medianUniqueMoviesPerUser", qualityStats.getDouble(1))
                   .addMetric("maxRatingsPerUser", qualityStats.getLong(2))
                   .addMetric("maxUniqueMoviesPerUser", qualityStats.getLong(3));

            // Calculate engagement consistency - exact same as original
            double engagementConsistency = 100.0;
            if (engagementStats.get(2) != null && !engagementStats.isNullAt(2)) {
                double avgVariance = engagementStats.getDouble(2);
                if (avgVariance > 2.0) engagementConsistency -= 30;
                else if (avgVariance > 1.5) engagementConsistency -= 20;
                else if (avgVariance > 1.0) engagementConsistency -= 10;
            }

            metrics.addMetric("engagementConsistencyScore", engagementConsistency);

        } catch (Exception e) {
            log.error("Error collecting user engagement analytics: {}", e.getMessage(), e);
            throw new AnalyticsCollectionException("USER_ENGAGEMENT", e.getMessage(), e);
        }
    }
    
    @Override
    public String getAnalyticsType() {
        return "USER_ENGAGEMENT";
    }
    
    @Override
    public boolean canProcess(Dataset<Row> ratingsDf, Dataset<Row> moviesData, Dataset<Row> tagsData) {
        return ratingsDf != null && !ratingsDf.isEmpty();
    }
    
    @Override
    public int getPriority() {
        return 25; // Medium-high priority for user engagement
    }
}
