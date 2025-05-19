package org.hiast.batch.application.pipeline.filters;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hiast.batch.application.pipeline.ALSTrainingPipelineContext;
import org.hiast.batch.application.pipeline.Filter;
import org.hiast.batch.application.port.out.AnalyticsPersistencePort;
import org.hiast.batch.domain.model.AnalyticsType;
import org.hiast.batch.domain.model.DataAnalytics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * Filter that collects and saves analytics data about the dataset.
 */
public class DataAnalyticsFilter implements Filter<ALSTrainingPipelineContext, ALSTrainingPipelineContext> {
    private static final Logger log = LoggerFactory.getLogger(DataAnalyticsFilter.class);
    
    private final AnalyticsPersistencePort analyticsPersistence;
    
    public DataAnalyticsFilter(AnalyticsPersistencePort analyticsPersistence) {
        this.analyticsPersistence = analyticsPersistence;
    }
    
    @Override
    public ALSTrainingPipelineContext process(ALSTrainingPipelineContext context) {
        log.info("Collecting and saving data analytics...");
        
        Dataset<Row> ratingsDf = context.getRatingsDf();
        
        if (ratingsDf == null || ratingsDf.isEmpty()) {
            log.warn("Ratings data is empty or null, skipping analytics collection.");
            return context;
        }
        
        try {
            // Collect user activity analytics
            collectUserActivityAnalytics(ratingsDf);
            
            // Collect movie popularity analytics
            collectMoviePopularityAnalytics(ratingsDf);
            
            // Collect rating distribution analytics
            collectRatingDistributionAnalytics(ratingsDf);
            
            log.info("Data analytics collection and saving completed successfully.");
        } catch (Exception e) {
            log.error("Error during data analytics collection: {}", e.getMessage(), e);
        }
        
        return context;
    }
    
    /**
     * Collects and saves analytics about user activity.
     */
    private void collectUserActivityAnalytics(Dataset<Row> ratingsDf) {
        log.info("Collecting user activity analytics...");
        
        // Get user activity metrics
        Dataset<Row> userActivity = ratingsDf.groupBy("userId")
                .count()
                .withColumnRenamed("count", "ratingsCount");
        
        // Calculate statistics
        Row userStats = userActivity.agg(
                functions.count("userId").as("totalUsers"),
                functions.avg("ratingsCount").as("avgRatingsPerUser"),
                functions.min("ratingsCount").as("minRatingsPerUser"),
                functions.max("ratingsCount").as("maxRatingsPerUser"),
                functions.expr("percentile(ratingsCount, 0.5)").as("medianRatingsPerUser")
        ).first();
        
        // Create metrics map
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("totalUsers", userStats.getLong(0));
        metrics.put("avgRatingsPerUser", userStats.getDouble(1));
        metrics.put("minRatingsPerUser", userStats.getLong(2));
        metrics.put("maxRatingsPerUser", userStats.getLong(3));
        metrics.put("medianRatingsPerUser", userStats.getDouble(4));
        
        // Add distribution data
        Dataset<Row> userDistribution = userActivity
                .groupBy(functions.expr("floor(ratingsCount/10)*10").as("ratingsBucket"))
                .count()
                .orderBy("ratingsBucket");
        
        // Convert distribution to a map
        userDistribution.collectAsList().forEach(row -> {
            String bucketKey = "usersBucket_" + row.getLong(0);
            metrics.put(bucketKey, row.getLong(1));
        });
        
        // Create and save analytics
        DataAnalytics analytics = new DataAnalytics(
                "user_activity_" + UUID.randomUUID().toString().substring(0, 8),
                Instant.now(),
                AnalyticsType.USER_ACTIVITY,
                metrics,
                "User activity analytics showing rating patterns"
        );
        
        boolean saved = analyticsPersistence.saveDataAnalytics(analytics);
        log.info("User activity analytics saved: {}", saved);
    }
    
    /**
     * Collects and saves analytics about movie popularity.
     */
    private void collectMoviePopularityAnalytics(Dataset<Row> ratingsDf) {
        log.info("Collecting movie popularity analytics...");
        
        // Get movie popularity metrics
        Dataset<Row> moviePopularity = ratingsDf.groupBy("movieId")
                .count()
                .withColumnRenamed("count", "ratingsCount");
        
        // Calculate statistics
        Row movieStats = moviePopularity.agg(
                functions.count("movieId").as("totalMovies"),
                functions.avg("ratingsCount").as("avgRatingsPerMovie"),
                functions.min("ratingsCount").as("minRatingsPerMovie"),
                functions.max("ratingsCount").as("maxRatingsPerMovie"),
                functions.expr("percentile(ratingsCount, 0.5)").as("medianRatingsPerMovie")
        ).first();
        
        // Create metrics map
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("totalMovies", movieStats.getLong(0));
        metrics.put("avgRatingsPerMovie", movieStats.getDouble(1));
        metrics.put("minRatingsPerMovie", movieStats.getLong(2));
        metrics.put("maxRatingsPerMovie", movieStats.getLong(3));
        metrics.put("medianRatingsPerMovie", movieStats.getDouble(4));
        
        // Get top 20 most popular movies
        Dataset<Row> topMovies = moviePopularity.orderBy(functions.desc("ratingsCount")).limit(20);
        
        // Add top movies to metrics
        topMovies.collectAsList().forEach(row -> {
            String movieKey = "topMovie_" + row.getInt(0);
            metrics.put(movieKey, row.getLong(1));
        });
        
        // Create and save analytics
        DataAnalytics analytics = new DataAnalytics(
                "movie_popularity_" + UUID.randomUUID().toString().substring(0, 8),
                Instant.now(),
                AnalyticsType.MOVIE_POPULARITY,
                metrics,
                "Movie popularity analytics showing rating patterns"
        );
        
        boolean saved = analyticsPersistence.saveDataAnalytics(analytics);
        log.info("Movie popularity analytics saved: {}", saved);
    }
    
    /**
     * Collects and saves analytics about rating distribution.
     */
    private void collectRatingDistributionAnalytics(Dataset<Row> ratingsDf) {
        log.info("Collecting rating distribution analytics...");
        
        // Get rating distribution
        Dataset<Row> ratingDistribution = ratingsDf.groupBy("ratingActual")
                .count()
                .orderBy("ratingActual");
        
        // Calculate statistics
        Row ratingStats = ratingsDf.agg(
                functions.count("ratingActual").as("totalRatings"),
                functions.avg("ratingActual").as("avgRating"),
                functions.min("ratingActual").as("minRating"),
                functions.max("ratingActual").as("maxRating"),
                functions.stddev("ratingActual").as("stdDevRating")
        ).first();
        
        // Create metrics map
        Map<String, Object> metrics = new HashMap<>();
        metrics.put("totalRatings", ratingStats.getLong(0));
        metrics.put("avgRating", ratingStats.getDouble(1));
        metrics.put("minRating", ratingStats.getDouble(2));
        metrics.put("maxRating", ratingStats.getDouble(3));
        metrics.put("stdDevRating", ratingStats.getDouble(4));
        
        // Add distribution data
        ratingDistribution.collectAsList().forEach(row -> {
            String ratingKey = "rating_" + row.getDouble(0);
            metrics.put(ratingKey, row.getLong(1));
        });
        
        // Create and save analytics
        DataAnalytics analytics = new DataAnalytics(
                "rating_distribution_" + UUID.randomUUID().toString().substring(0, 8),
                Instant.now(),
                AnalyticsType.RATING_DISTRIBUTION,
                metrics,
                "Rating distribution analytics showing rating patterns"
        );
        
        boolean saved = analyticsPersistence.saveDataAnalytics(analytics);
        log.info("Rating distribution analytics saved: {}", saved);
    }
}
