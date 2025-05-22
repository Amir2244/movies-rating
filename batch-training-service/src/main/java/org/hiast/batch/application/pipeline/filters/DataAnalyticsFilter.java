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
        Dataset<Row> moviesData = context.getRawMovies();
        Dataset<Row> tagsData = context.getRawTags();

        if (ratingsDf == null || ratingsDf.isEmpty()) {
            log.warn("Ratings data is empty or null, skipping analytics collection.");
            return context;
        }

        // Register temporary views for easier SQL access
        if (moviesData != null && !moviesData.isEmpty()) {
            moviesData.createOrReplaceTempView("movies");
            log.info("Registered movies data as temporary view 'movies'");
        } else {
            log.warn("Movies data is empty or null, genre analytics will be limited");
        }

        if (tagsData != null && !tagsData.isEmpty()) {
            tagsData.createOrReplaceTempView("tags");
            log.info("Registered tags data as temporary view 'tags'");
        } else {
            log.warn("Tags data is empty or null, tag-based analytics will be unavailable");
        }

        try {
            // === EXISTING ANALYTICS ===
            // Collect user activity analytics
            collectUserActivityAnalytics(ratingsDf);

            // Collect movie popularity analytics
            collectMoviePopularityAnalytics(ratingsDf);

            // Collect rating distribution analytics
            collectRatingDistributionAnalytics(ratingsDf);

            // === NEW COMPREHENSIVE ANALYTICS ===
            // Collect data quality analytics
            collectDataCompletenessAnalytics(ratingsDf);
            collectDataFreshnessAnalytics(ratingsDf);

            // Collect enhanced user behavior analytics
            collectUserEngagementAnalytics(ratingsDf);
            collectUserSegmentationAnalytics(ratingsDf);

            // Collect content analytics
            collectGenreDistributionAnalytics(ratingsDf, moviesData);
            collectTemporalTrendsAnalytics(ratingsDf);
            collectContentPerformanceAnalytics(ratingsDf, moviesData);

            // Collect system performance analytics
            collectProcessingPerformanceAnalytics(ratingsDf, context);

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

    // === NEW COMPREHENSIVE ANALYTICS METHODS ===

    /**
     * Collects and saves analytics about data completeness and quality.
     */
    private void collectDataCompletenessAnalytics(Dataset<Row> ratingsDf) {
        log.info("Collecting data completeness analytics...");

        try {
            // Calculate completeness metrics
            long totalRows = ratingsDf.count();
            long nonNullUserIds = ratingsDf.filter(functions.col("userId").isNotNull()).count();
            long nonNullMovieIds = ratingsDf.filter(functions.col("movieId").isNotNull()).count();
            long nonNullRatings = ratingsDf.filter(functions.col("ratingActual").isNotNull()).count();
            long nonNullTimestamps = ratingsDf.filter(functions.col("timestamp").isNotNull()).count();

            // Calculate completeness percentages
            double userIdCompleteness = (double) nonNullUserIds / totalRows * 100;
            double movieIdCompleteness = (double) nonNullMovieIds / totalRows * 100;
            double ratingCompleteness = (double) nonNullRatings / totalRows * 100;
            double timestampCompleteness = (double) nonNullTimestamps / totalRows * 100;

            // Create metrics map
            Map<String, Object> metrics = new HashMap<>();
            metrics.put("totalRows", totalRows);
            metrics.put("userIdCompleteness", userIdCompleteness);
            metrics.put("movieIdCompleteness", movieIdCompleteness);
            metrics.put("ratingCompleteness", ratingCompleteness);
            metrics.put("timestampCompleteness", timestampCompleteness);
            metrics.put("overallCompleteness", (userIdCompleteness + movieIdCompleteness + ratingCompleteness + timestampCompleteness) / 4);

            // Check for duplicate records
            long uniqueRecords = ratingsDf.dropDuplicates().count();
            long duplicateRecords = totalRows - uniqueRecords;
            metrics.put("uniqueRecords", uniqueRecords);
            metrics.put("duplicateRecords", duplicateRecords);
            metrics.put("duplicatePercentage", (double) duplicateRecords / totalRows * 100);

            // Create and save analytics
            DataAnalytics analytics = new DataAnalytics(
                    "data_completeness_" + UUID.randomUUID().toString().substring(0, 8),
                    Instant.now(),
                    AnalyticsType.DATA_COMPLETENESS,
                    metrics,
                    "Data completeness and quality analytics"
            );

            boolean saved = analyticsPersistence.saveDataAnalytics(analytics);
            log.info("Data completeness analytics saved: {}", saved);
        } catch (Exception e) {
            log.error("Error collecting data completeness analytics: {}", e.getMessage(), e);
        }
    }

    /**
     * Collects and saves analytics about data freshness and recency.
     */
    private void collectDataFreshnessAnalytics(Dataset<Row> ratingsDf) {
        log.info("Collecting data freshness analytics...");

        try {
            // Calculate freshness metrics based on timestamp
            Row freshnessStats = ratingsDf.agg(
                    functions.min("timestamp").as("oldestRating"),
                    functions.max("timestamp").as("newestRating"),
                    functions.count("timestamp").as("totalRatings")
            ).first();

            long oldestTimestamp = freshnessStats.getLong(0);
            long newestTimestamp = freshnessStats.getLong(1);
            long totalRatings = freshnessStats.getLong(2);

            // Calculate time spans
            long timeSpanSeconds = newestTimestamp - oldestTimestamp;
            long timeSpanDays = timeSpanSeconds / (24 * 60 * 60);

            // Calculate ratings per time period
            double ratingsPerDay = totalRatings / (double) Math.max(timeSpanDays, 1);
            double ratingsPerHour = ratingsPerDay / 24;

            // Create metrics map
            Map<String, Object> metrics = new HashMap<>();
            metrics.put("oldestRatingTimestamp", oldestTimestamp);
            metrics.put("newestRatingTimestamp", newestTimestamp);
            metrics.put("timeSpanDays", timeSpanDays);
            metrics.put("ratingsPerDay", ratingsPerDay);
            metrics.put("ratingsPerHour", ratingsPerHour);

            // Calculate recent activity (last 30 days equivalent in seconds)
            long thirtyDaysAgo = newestTimestamp - (30L * 24 * 60 * 60);
            long recentRatings = ratingsDf.filter(functions.col("timestamp").gt(thirtyDaysAgo)).count();
            metrics.put("recentRatings30Days", recentRatings);
            metrics.put("recentActivityPercentage", (double) recentRatings / totalRatings * 100);

            // Create and save analytics
            DataAnalytics analytics = new DataAnalytics(
                    "data_freshness_" + UUID.randomUUID().toString().substring(0, 8),
                    Instant.now(),
                    AnalyticsType.DATA_FRESHNESS,
                    metrics,
                    "Data freshness and recency analytics"
            );

            boolean saved = analyticsPersistence.saveDataAnalytics(analytics);
            log.info("Data freshness analytics saved: {}", saved);
        } catch (Exception e) {
            log.error("Error collecting data freshness analytics: {}", e.getMessage(), e);
        }
    }

    /**
     * Collects and saves analytics about user engagement depth and quality.
     */
    private void collectUserEngagementAnalytics(Dataset<Row> ratingsDf) {
        log.info("Collecting user engagement analytics...");

        try {
            // Calculate engagement metrics
            Dataset<Row> userEngagement = ratingsDf.groupBy("userId")
                    .agg(
                            functions.count("ratingActual").as("totalRatings"),
                            functions.avg("ratingActual").as("avgRating"),
                            functions.stddev("ratingActual").as("ratingVariance"),
                            functions.countDistinct("movieId").as("uniqueMovies"),
                            functions.max("timestamp").minus(functions.min("timestamp")).as("engagementSpan")
                    );

            // Calculate engagement statistics
            Row engagementStats = userEngagement.agg(
                    functions.avg("totalRatings").as("avgRatingsPerUser"),
                    functions.avg("avgRating").as("avgUserRating"),
                    functions.avg("ratingVariance").as("avgRatingVariance"),
                    functions.avg("uniqueMovies").as("avgUniqueMoviesPerUser"),
                    functions.avg("engagementSpan").as("avgEngagementSpan")
            ).first();

            // Create metrics map
            Map<String, Object> metrics = new HashMap<>();
            metrics.put("avgRatingsPerUser", engagementStats.getDouble(0));
            metrics.put("avgUserRating", engagementStats.getDouble(1));
            metrics.put("avgRatingVariance", engagementStats.getDouble(2));
            metrics.put("avgUniqueMoviesPerUser", engagementStats.getDouble(3));
            metrics.put("avgEngagementSpanSeconds", engagementStats.getDouble(4));

            // Calculate engagement levels
            long highlyEngagedUsers = userEngagement.filter(functions.col("totalRatings").gt(50)).count();
            long moderatelyEngagedUsers = userEngagement.filter(functions.col("totalRatings").between(10, 50)).count();
            long lowEngagedUsers = userEngagement.filter(functions.col("totalRatings").lt(10)).count();

            metrics.put("highlyEngagedUsers", highlyEngagedUsers);
            metrics.put("moderatelyEngagedUsers", moderatelyEngagedUsers);
            metrics.put("lowEngagedUsers", lowEngagedUsers);

            // Create and save analytics
            DataAnalytics analytics = new DataAnalytics(
                    "user_engagement_" + UUID.randomUUID().toString().substring(0, 8),
                    Instant.now(),
                    AnalyticsType.USER_ENGAGEMENT,
                    metrics,
                    "User engagement depth and quality analytics"
            );

            boolean saved = analyticsPersistence.saveDataAnalytics(analytics);
            log.info("User engagement analytics saved: {}", saved);
        } catch (Exception e) {
            log.error("Error collecting user engagement analytics: {}", e.getMessage(), e);
        }
    }

    /**
     * Collects and saves analytics about user segmentation based on behavior patterns.
     */
    private void collectUserSegmentationAnalytics(Dataset<Row> ratingsDf) {
        log.info("Collecting user segmentation analytics...");

        try {
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

            // Create metrics map
            Map<String, Object> metrics = new HashMap<>();

            // Add segment counts
            segmentCounts.collectAsList().forEach(row -> {
                String segmentKey = row.getString(0) + "_" + row.getString(1);
                metrics.put("segment_" + segmentKey, row.getLong(2));
            });

            // Calculate overall segment statistics
            long totalUsers = userBehavior.count();
            long highActivityUsers = userSegments.filter(functions.col("activityLevel").equalTo("High")).count();
            long mediumActivityUsers = userSegments.filter(functions.col("activityLevel").equalTo("Medium")).count();
            long lowActivityUsers = userSegments.filter(functions.col("activityLevel").equalTo("Low")).count();

            metrics.put("totalUsers", totalUsers);
            metrics.put("highActivityUsers", highActivityUsers);
            metrics.put("mediumActivityUsers", mediumActivityUsers);
            metrics.put("lowActivityUsers", lowActivityUsers);
            metrics.put("highActivityPercentage", (double) highActivityUsers / totalUsers * 100);

            // Create and save analytics
            DataAnalytics analytics = new DataAnalytics(
                    "user_segmentation_" + UUID.randomUUID().toString().substring(0, 8),
                    Instant.now(),
                    AnalyticsType.USER_SEGMENTATION,
                    metrics,
                    "User segmentation analytics based on behavior patterns"
            );

            boolean saved = analyticsPersistence.saveDataAnalytics(analytics);
            log.info("User segmentation analytics saved: {}", saved);
        } catch (Exception e) {
            log.error("Error collecting user segmentation analytics: {}", e.getMessage(), e);
        }
    }

    /**
     * Collects and saves analytics about genre distribution and preferences.
     * Uses actual movie metadata from movies.csv to provide comprehensive genre analysis.
     *
     * @param ratingsDf The ratings DataFrame
     * @param moviesData The movies DataFrame containing genre information
     */
    private void collectGenreDistributionAnalytics(Dataset<Row> ratingsDf, Dataset<Row> moviesData) {
        log.info("Collecting genre distribution analytics...");

        try {
            Map<String, Object> metrics = new HashMap<>();

            // Use the movies data passed from the context

            if (moviesData != null && !moviesData.isEmpty()) {
                log.info("Using actual movie metadata for genre analysis");

                // Join ratings with movies to get genre information
                Dataset<Row> ratingsWithGenres = ratingsDf
                        .join(moviesData, ratingsDf.col("movieId").equalTo(moviesData.col("movieId")), "inner")
                        .select(ratingsDf.col("*"), moviesData.col("genres"));

                // Split genres and create individual genre records
                // MovieLens genres are pipe-separated (e.g., "Action|Adventure|Sci-Fi")
                Dataset<Row> genreRatings = ratingsWithGenres
                        .withColumn("genre", functions.explode(functions.split(functions.col("genres"), "\\|")))
                        .filter(functions.col("genre").notEqual("(no genres listed)"));

                // Calculate genre statistics
                Dataset<Row> genreStats = genreRatings.groupBy("genre")
                        .agg(
                                functions.count("ratingActual").as("ratingCount"),
                                functions.avg("ratingActual").as("avgRating"),
                                functions.countDistinct("userId").as("uniqueUsers"),
                                functions.countDistinct("movieId").as("uniqueMovies"),
                                functions.stddev("ratingActual").as("ratingStdDev")
                        )
                        .orderBy(functions.desc("ratingCount"));

                // Add genre statistics to metrics
                genreStats.collectAsList().forEach(row -> {
                    String genre = row.getString(0).replaceAll("[^a-zA-Z0-9]", ""); // Clean genre name for key
                    metrics.put("genre_" + genre + "_count", row.getLong(1));
                    metrics.put("genre_" + genre + "_avgRating", row.getDouble(2));
                    metrics.put("genre_" + genre + "_uniqueUsers", row.getLong(3));
                    metrics.put("genre_" + genre + "_uniqueMovies", row.getLong(4));
                    metrics.put("genre_" + genre + "_ratingStdDev", row.getDouble(5));
                });

                // Calculate genre diversity metrics
                long totalGenres = genreStats.count();
                metrics.put("totalGenres", totalGenres);
                metrics.put("genreDataAvailable", true);

                // Find most and least popular genres
                Row mostPopular = genreStats.first();
                Row leastPopular = genreStats.orderBy(functions.asc("ratingCount")).first();

                metrics.put("mostPopularGenre", mostPopular.getString(0));
                metrics.put("mostPopularGenreCount", mostPopular.getLong(1));
                metrics.put("leastPopularGenre", leastPopular.getString(0));
                metrics.put("leastPopularGenreCount", leastPopular.getLong(1));

                // Calculate genre rating quality
                Row bestRated = genreStats.orderBy(functions.desc("avgRating")).first();
                Row worstRated = genreStats.orderBy(functions.asc("avgRating")).first();

                metrics.put("bestRatedGenre", bestRated.getString(0));
                metrics.put("bestRatedGenreAvg", bestRated.getDouble(2));
                metrics.put("worstRatedGenre", worstRated.getString(0));
                metrics.put("worstRatedGenreAvg", worstRated.getDouble(2));

            } else {
                log.warn("Movies metadata not available - using fallback genre analysis");

                // Fallback implementation when movies data is not available
                metrics.put("genreDataAvailable", false);
                metrics.put("note", "Movies metadata not available - genre analysis limited");

                // Provide basic movie-based distribution analytics
                Dataset<Row> movieStats = ratingsDf.groupBy("movieId")
                        .agg(
                                functions.count("ratingActual").as("ratingCount"),
                                functions.avg("ratingActual").as("avgRating")
                        );

                Row movieDistribution = movieStats.agg(
                        functions.count("movieId").as("totalMovies"),
                        functions.avg("ratingCount").as("avgRatingsPerMovie"),
                        functions.avg("avgRating").as("avgMovieRating")
                ).first();

                metrics.put("totalMovies", movieDistribution.getLong(0));
                metrics.put("avgRatingsPerMovie", movieDistribution.getDouble(1));
                metrics.put("avgMovieRating", movieDistribution.getDouble(2));
            }

            // Create and save analytics
            DataAnalytics analytics = new DataAnalytics(
                    "genre_distribution_" + UUID.randomUUID().toString().substring(0, 8),
                    Instant.now(),
                    AnalyticsType.GENRE_DISTRIBUTION,
                    metrics,
                    "Genre distribution and preference analytics using MovieLens metadata"
            );

            boolean saved = analyticsPersistence.saveDataAnalytics(analytics);
            log.info("Genre distribution analytics saved: {}", saved);
        } catch (Exception e) {
            log.error("Error collecting genre distribution analytics: {}", e.getMessage(), e);
        }
    }

    /**
     * Collects and saves analytics about temporal trends and patterns.
     */
    private void collectTemporalTrendsAnalytics(Dataset<Row> ratingsDf) {
        log.info("Collecting temporal trends analytics...");

        try {
            // Convert timestamp to readable date components
            Dataset<Row> temporalData = ratingsDf
                    .withColumn("date", functions.from_unixtime(functions.col("timestamp")))
                    .withColumn("year", functions.year(functions.from_unixtime(functions.col("timestamp"))))
                    .withColumn("month", functions.month(functions.from_unixtime(functions.col("timestamp"))))
                    .withColumn("dayOfWeek", functions.dayofweek(functions.from_unixtime(functions.col("timestamp"))))
                    .withColumn("hour", functions.hour(functions.from_unixtime(functions.col("timestamp"))));

            // Analyze trends by year
            Dataset<Row> yearlyTrends = temporalData.groupBy("year")
                    .agg(
                            functions.count("ratingActual").as("ratingCount"),
                            functions.avg("ratingActual").as("avgRating"),
                            functions.countDistinct("userId").as("activeUsers")
                    )
                    .orderBy("year");

            // Analyze trends by month
            Dataset<Row> monthlyTrends = temporalData.groupBy("month")
                    .agg(
                            functions.count("ratingActual").as("ratingCount"),
                            functions.avg("ratingActual").as("avgRating")
                    )
                    .orderBy("month");

            // Analyze trends by day of week
            Dataset<Row> weeklyTrends = temporalData.groupBy("dayOfWeek")
                    .agg(
                            functions.count("ratingActual").as("ratingCount"),
                            functions.avg("ratingActual").as("avgRating")
                    )
                    .orderBy("dayOfWeek");

            // Analyze trends by hour
            Dataset<Row> hourlyTrends = temporalData.groupBy("hour")
                    .agg(
                            functions.count("ratingActual").as("ratingCount"),
                            functions.avg("ratingActual").as("avgRating")
                    )
                    .orderBy("hour");

            // Create metrics map
            Map<String, Object> metrics = new HashMap<>();

            // Add yearly trends
            yearlyTrends.collectAsList().forEach(row -> {
                int year = row.getInt(0);
                metrics.put("year_" + year + "_count", row.getLong(1));
                metrics.put("year_" + year + "_avgRating", row.getDouble(2));
                metrics.put("year_" + year + "_activeUsers", row.getLong(3));
            });

            // Add monthly trends
            monthlyTrends.collectAsList().forEach(row -> {
                int month = row.getInt(0);
                metrics.put("month_" + month + "_count", row.getLong(1));
                metrics.put("month_" + month + "_avgRating", row.getDouble(2));
            });

            // Add weekly trends
            weeklyTrends.collectAsList().forEach(row -> {
                int dayOfWeek = row.getInt(0);
                metrics.put("dayOfWeek_" + dayOfWeek + "_count", row.getLong(1));
                metrics.put("dayOfWeek_" + dayOfWeek + "_avgRating", row.getDouble(2));
            });

            // Add hourly trends (top 5 most active hours)
            hourlyTrends.limit(5).collectAsList().forEach(row -> {
                int hour = row.getInt(0);
                metrics.put("topHour_" + hour + "_count", row.getLong(1));
                metrics.put("topHour_" + hour + "_avgRating", row.getDouble(2));
            });

            // Create and save analytics
            DataAnalytics analytics = new DataAnalytics(
                    "temporal_trends_" + UUID.randomUUID().toString().substring(0, 8),
                    Instant.now(),
                    AnalyticsType.TEMPORAL_TRENDS,
                    metrics,
                    "Temporal trends and seasonal pattern analytics"
            );

            boolean saved = analyticsPersistence.saveDataAnalytics(analytics);
            log.info("Temporal trends analytics saved: {}", saved);
        } catch (Exception e) {
            log.error("Error collecting temporal trends analytics: {}", e.getMessage(), e);
        }
    }

    /**
     * Collects and saves analytics about overall content performance and reception.
     * Uses movie metadata when available for enhanced content analytics.
     *
     * @param ratingsDf The ratings DataFrame
     * @param moviesData The movies DataFrame containing title and genre information
     */
    private void collectContentPerformanceAnalytics(Dataset<Row> ratingsDf, Dataset<Row> moviesData) {
        log.info("Collecting content performance analytics...");

        try {
            // Analyze movie performance metrics
            Dataset<Row> moviePerformance;

            if (moviesData != null && !moviesData.isEmpty()) {
                // Join with movies data to get titles and genres
                moviePerformance = ratingsDf.groupBy("movieId")
                        .agg(
                                functions.count("ratingActual").as("totalRatings"),
                                functions.avg("ratingActual").as("avgRating"),
                                functions.stddev("ratingActual").as("ratingStdDev"),
                                functions.countDistinct("userId").as("uniqueRaters"),
                                functions.min("ratingActual").as("minRating"),
                                functions.max("ratingActual").as("maxRating")
                        )
                        .join(moviesData, "movieId")
                        .select(
                                functions.col("movieId"),
                                functions.col("title"),
                                functions.col("genres"),
                                functions.col("totalRatings"),
                                functions.col("avgRating"),
                                functions.col("ratingStdDev"),
                                functions.col("uniqueRaters"),
                                functions.col("minRating"),
                                functions.col("maxRating")
                        );

                log.info("Enhanced content performance analytics with movie titles and genres");
            } else {
                // Basic performance metrics without movie metadata
                moviePerformance = ratingsDf.groupBy("movieId")
                        .agg(
                                functions.count("ratingActual").as("totalRatings"),
                                functions.avg("ratingActual").as("avgRating"),
                                functions.stddev("ratingActual").as("ratingStdDev"),
                                functions.countDistinct("userId").as("uniqueRaters"),
                                functions.min("ratingActual").as("minRating"),
                                functions.max("ratingActual").as("maxRating")
                        );

                log.info("Basic content performance analytics without movie metadata");
            }

            // Calculate performance statistics
            Row performanceStats = moviePerformance.agg(
                    functions.count("movieId").as("totalMovies"),
                    functions.avg("totalRatings").as("avgRatingsPerMovie"),
                    functions.avg("avgRating").as("overallAvgRating"),
                    functions.avg("ratingStdDev").as("avgRatingVariability"),
                    functions.avg("uniqueRaters").as("avgUniqueRatersPerMovie")
            ).first();

            // Identify top and bottom performing content
            Dataset<Row> topPerformers = moviePerformance
                    .filter(functions.col("totalRatings").gt(10)) // Only movies with sufficient ratings
                    .orderBy(functions.desc("avgRating"), functions.desc("totalRatings"))
                    .limit(10);

            Dataset<Row> bottomPerformers = moviePerformance
                    .filter(functions.col("totalRatings").gt(10))
                    .orderBy(functions.asc("avgRating"), functions.desc("totalRatings"))
                    .limit(10);

            // Create metrics map
            Map<String, Object> metrics = new HashMap<>();

            // Add overall performance statistics
            metrics.put("totalMovies", performanceStats.getLong(0));
            metrics.put("avgRatingsPerMovie", performanceStats.getDouble(1));
            metrics.put("overallAvgRating", performanceStats.getDouble(2));
            metrics.put("avgRatingVariability", performanceStats.getDouble(3));
            metrics.put("avgUniqueRatersPerMovie", performanceStats.getDouble(4));

            // Add top performers with titles if available
            if (moviesData != null && !moviesData.isEmpty()) {
                topPerformers.collectAsList().forEach(row -> {
                    int movieId = row.getInt(0);
                    String title = row.getString(1);
                    String safeTitle = title.replaceAll("[^a-zA-Z0-9]", "").substring(0, Math.min(20, title.length()));

                    metrics.put("topMovie_" + movieId + "_title", title);
                    metrics.put("topMovie_" + movieId + "_avgRating", row.getDouble(4));
                    metrics.put("topMovie_" + movieId + "_totalRatings", row.getLong(3));
                    metrics.put("topMovie_" + movieId + "_genres", row.getString(2));
                });

                // Add bottom performers with titles
                bottomPerformers.collectAsList().forEach(row -> {
                    int movieId = row.getInt(0);
                    String title = row.getString(1);
                    String safeTitle = title.replaceAll("[^a-zA-Z0-9]", "").substring(0, Math.min(20, title.length()));

                    metrics.put("bottomMovie_" + movieId + "_title", title);
                    metrics.put("bottomMovie_" + movieId + "_avgRating", row.getDouble(4));
                    metrics.put("bottomMovie_" + movieId + "_totalRatings", row.getLong(3));
                    metrics.put("bottomMovie_" + movieId + "_genres", row.getString(2));
                });
            } else {
                // Basic metrics without titles
                topPerformers.collectAsList().forEach(row -> {
                    int movieId = row.getInt(0);
                    metrics.put("topMovie_" + movieId + "_avgRating", row.getDouble(2));
                    metrics.put("topMovie_" + movieId + "_totalRatings", row.getLong(1));
                });

                // Add bottom performers
                bottomPerformers.collectAsList().forEach(row -> {
                    int movieId = row.getInt(0);
                    metrics.put("bottomMovie_" + movieId + "_avgRating", row.getDouble(2));
                    metrics.put("bottomMovie_" + movieId + "_totalRatings", row.getLong(1));
                });
            }

            // Calculate content diversity metrics
            long highRatedMovies = moviePerformance.filter(functions.col("avgRating").gt(4.0)).count();
            long lowRatedMovies = moviePerformance.filter(functions.col("avgRating").lt(2.5)).count();
            long polarizingMovies = moviePerformance.filter(functions.col("ratingStdDev").gt(1.5)).count();

            metrics.put("highRatedMovies", highRatedMovies);
            metrics.put("lowRatedMovies", lowRatedMovies);
            metrics.put("polarizingMovies", polarizingMovies);

            // Create and save analytics
            DataAnalytics analytics = new DataAnalytics(
                    "content_performance_" + UUID.randomUUID().toString().substring(0, 8),
                    Instant.now(),
                    AnalyticsType.CONTENT_PERFORMANCE,
                    metrics,
                    "Overall content performance and reception analytics"
            );

            boolean saved = analyticsPersistence.saveDataAnalytics(analytics);
            log.info("Content performance analytics saved: {}", saved);
        } catch (Exception e) {
            log.error("Error collecting content performance analytics: {}", e.getMessage(), e);
        }
    }

    /**
     * Collects and saves analytics about batch processing performance and efficiency.
     */
    private void collectProcessingPerformanceAnalytics(Dataset<Row> ratingsDf, ALSTrainingPipelineContext context) {
        log.info("Collecting processing performance analytics...");

        try {
            long startTime = System.currentTimeMillis();

            // Basic dataset metrics
            long totalRecords = ratingsDf.count();
            int partitionCount = ratingsDf.rdd().getNumPartitions();

            // Calculate processing metrics
            long processingTime = System.currentTimeMillis() - startTime;
            double recordsPerSecond = totalRecords / (processingTime / 1000.0);

            // Memory and storage metrics (approximations)
            long estimatedMemoryUsage = ratingsDf.storageLevel().useMemory() ?
                    totalRecords * 32 : 0;

            // Create metrics map
            Map<String, Object> metrics = new HashMap<>();
            metrics.put("totalRecords", totalRecords);
            metrics.put("partitionCount", partitionCount);
            metrics.put("processingTimeMs", processingTime);
            metrics.put("recordsPerSecond", recordsPerSecond);
            metrics.put("estimatedMemoryUsageBytes", estimatedMemoryUsage);
            metrics.put("recordsPerPartition", totalRecords / partitionCount);

            // Data quality indicators that affect performance
            long nullRecords = ratingsDf.filter(
                    functions.col("userId").isNull()
                            .or(functions.col("movieId").isNull())
                            .or(functions.col("ratingActual").isNull())
            ).count();

            metrics.put("nullRecords", nullRecords);
            metrics.put("dataQualityScore", (double) (totalRecords - nullRecords) / totalRecords * 100);

            // Processing efficiency metrics
            metrics.put("processingEfficiency", recordsPerSecond > 1000 ? "High" :
                    recordsPerSecond > 100 ? "Medium" : "Low");

            // Create and save analytics
            DataAnalytics analytics = new DataAnalytics(
                    "processing_performance_" + UUID.randomUUID().toString().substring(0, 8),
                    Instant.now(),
                    AnalyticsType.PROCESSING_PERFORMANCE,
                    metrics,
                    "Batch processing performance and efficiency analytics"
            );

            boolean saved = analyticsPersistence.saveDataAnalytics(analytics);
            log.info("Processing performance analytics saved: {}", saved);
        } catch (Exception e) {
            log.error("Error collecting processing performance analytics: {}", e.getMessage(), e);
        }
    }
}
