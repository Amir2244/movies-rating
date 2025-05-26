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
import java.util.*;

/**
 * Service responsible for collecting content-related analytics.
 * Handles movie popularity, genre distribution, and content performance analytics.
 * <p>
 * This service focuses on content analysis including movie performance,
 * genre preferences, and content diversity metrics.
 */
public class ContentAnalyticsService implements AnalyticsCollector {

    private static final Logger log = LoggerFactory.getLogger(ContentAnalyticsService.class);
    private final List<DataAnalytics> analytics = new ArrayList<>();

    @Override
    public List<DataAnalytics> collectAnalytics(Dataset<Row> ratingsDf,
                                                Dataset<Row> moviesData,
                                                Dataset<Row> tagsData,
                                                ALSTrainingPipelineContext context) {

        if (!canProcess(ratingsDf, moviesData, tagsData)) {
            throw new AnalyticsCollectionException("CONTENT_ANALYTICS", "Insufficient data for content analytics");
        }

        try {
            log.info("Collecting content analytics...");

            AnalyticsMetrics metrics = AnalyticsMetrics.builder();

            // Collect movie popularity metrics
            collectMoviePopularityMetrics(ratingsDf, moviesData, tagsData, metrics);

            // Collect genre distribution metrics if movies data is available
            if (moviesData != null && !moviesData.isEmpty()) {
                collectGenreDistributionMetrics(ratingsDf, moviesData, metrics);
            }

            // Collect content performance metrics
            collectContentPerformanceMetrics(ratingsDf, metrics);

            log.info("Content analytics collection completed with {} metrics", metrics.size());

            return analytics;

        } catch (Exception e) {
            log.error("Error collecting content analytics: {}", e.getMessage(), e);
            throw new AnalyticsCollectionException("CONTENT_ANALYTICS", e.getMessage(), e);
        }
    }

    /**
     * Collects movie popularity metrics with enhanced insights.
     */
    private void collectMoviePopularityMetrics(Dataset<Row> ratingsDf, Dataset<Row> moviesData,
                                               Dataset<Row> tagsData, AnalyticsMetrics metrics) {
        log.debug("Collecting movie popularity metrics...");
        metrics.clearMetrics();
        // Get movie popularity metrics with enhanced data
        Dataset<Row> moviePopularity = ratingsDf.groupBy("movieId")
                .agg(
                        functions.count("ratingActual").as("ratingsCount"),
                        functions.avg("ratingActual").as("avgRating"),
                        functions.stddev("ratingActual").as("ratingStdDev")
                );

        // Enhance with movie metadata if available
        if (moviesData != null && !moviesData.isEmpty()) {
            moviePopularity = moviePopularity
                    .join(moviesData, "movieId")
                    .select(
                            functions.col("movieId"),
                            functions.col("title"),
                            functions.col("genres"),
                            functions.col("ratingsCount"),
                            functions.col("avgRating"),
                            functions.col("ratingStdDev")
                    );
        }

        // Calculate statistics
        Row movieStats = moviePopularity.agg(
                functions.count("movieId").as("totalMovies"),
                functions.avg("ratingsCount").as("avgRatingsPerMovie"),
                functions.min("ratingsCount").as("minRatingsPerMovie"),
                functions.max("ratingsCount").as("maxRatingsPerMovie"),
                functions.expr("percentile(ratingsCount, 0.5)").as("medianRatingsPerMovie"),
                functions.avg("avgRating").as("overallAvgRating")
        ).first();

        metrics.addMetric("popularity_totalMovies", movieStats.getLong(0))
                .addMetric("popularity_avgRatingsPerMovie", movieStats.getDouble(1))
                .addMetric("popularity_minRatingsPerMovie", movieStats.getLong(2))
                .addMetric("popularity_maxRatingsPerMovie", movieStats.getLong(3))
                .addMetric("popularity_medianRatingsPerMovie", movieStats.getDouble(4))
                .addMetric("popularity_overallAvgRating", movieStats.getDouble(5));

        // Get top 10 most popular movies with enhanced information
        Dataset<Row> topMovies = moviePopularity.orderBy(functions.desc("ratingsCount")).limit(10);

        // Add top movies to metrics with enhanced data
        topMovies.collectAsList().forEach(row -> {
            int movieId = row.getInt(0);
            long ratingsCount = moviesData != null && !moviesData.isEmpty() ? row.getLong(3) : row.getLong(1);

            if (moviesData != null && !moviesData.isEmpty()) {
                String title = row.getString(1);
                String genres = row.getString(2);
                double avgRating = row.getDouble(4);

                metrics.addMetric("topMovie_" + movieId + "_title", title)
                        .addMetric("topMovie_" + movieId + "_genres", genres)
                        .addMetric("topMovie_" + movieId + "_ratingsCount", ratingsCount)
                        .addMetric("topMovie_" + movieId + "_avgRating", avgRating);
            } else {
                metrics.addMetric("topMovie_" + movieId + "_ratingsCount", ratingsCount);
            }
        });

        // Add user perspective insights through tags analysis if available
        if (tagsData != null && !tagsData.isEmpty()) {
            addTagInsights(topMovies, tagsData, metrics);
        }
        analytics.add(new DataAnalytics(
                "content_analytics_" + UUID.randomUUID().toString().substring(0, 8),
                Instant.now(),
                AnalyticsType.MOVIE_POPULARITY,
                metrics.build(),
                "Comprehensive content analytics including popularity, genres, and performance"
        ));
        log.debug("Movie popularity metrics collected");
    }

    /**
     * Adds tag insights for popular movies.
     */
    private void addTagInsights(Dataset<Row> topMovies, Dataset<Row> tagsData, AnalyticsMetrics metrics) {
        log.debug("Adding tag insights for popular movies...");

        try {
            // Get most popular movies with their associated tags
            Dataset<Row> topMovieIds = topMovies.select("movieId");

            // Join with tags to get user perspectives on popular movies
            Dataset<Row> popularMovieTags = topMovieIds
                    .join(tagsData, "movieId")
                    .groupBy("movieId", "tag")
                    .count()
                    .withColumnRenamed("count", "tagCount");

            // Get top tags for each popular movie
            Dataset<Row> topTagsPerMovie = popularMovieTags
                    .withColumn("rank", functions.row_number().over(
                            org.apache.spark.sql.expressions.Window
                                    .partitionBy("movieId")
                                    .orderBy(functions.desc("tagCount"))
                    ))
                    .filter(functions.col("rank").leq(3));

            // Add tag insights to metrics
            topTagsPerMovie.collectAsList().forEach(row -> {
                int movieId = row.getInt(0);
                String tag = row.getString(1);
                long tagCount = row.getLong(2);
                int rank = row.getInt(3);

                metrics.addMetric("topMovie_" + movieId + "_tag" + rank + "_name", tag)
                        .addMetric("topMovie_" + movieId + "_tag" + rank + "_count", tagCount);
            });

            metrics.addMetric("tagAnalysisAvailable", true);
        } catch (Exception e) {
            log.warn("Error adding tag insights: {}", e.getMessage());
            metrics.addMetric("tagAnalysisAvailable", false);
        }
    }

    /**
     * Collects genre distribution metrics.
     */
    private void collectGenreDistributionMetrics(Dataset<Row> ratingsDf, Dataset<Row> moviesData,
                                                 AnalyticsMetrics metrics) {
        metrics.clearMetrics();
        log.debug("Collecting genre distribution metrics...");
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
                    .filter(functions.col("genre").notEqual("(no genres listed)"))
                    .filter(functions.col("genre").isNotNull())
                    .filter(functions.length(functions.col("genre")).gt(0));

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
                try {
                    String genre = row.getString(0);
                    if (genre != null && !genre.trim().isEmpty()) {
                        String cleanGenre = genre.replaceAll("[^a-zA-Z0-9]", "").toLowerCase(); // Clean genre name for key
                        if (!cleanGenre.isEmpty()) {
                            metrics.addMetric("genre_" + cleanGenre + "_name", genre);
                            metrics.addMetric("genre_" + cleanGenre + "_count", row.getLong(1));
                            metrics.addMetric("genre_" + cleanGenre + "_avgRating", row.getDouble(2));
                            metrics.addMetric("genre_" + cleanGenre + "_uniqueUsers", row.getLong(3));
                            metrics.addMetric("genre_" + cleanGenre + "_uniqueMovies", row.getLong(4));

                            // Handle potential null stddev
                            Object stdDev = row.get(5);
                            if (stdDev != null && !row.isNullAt(5)) {
                                metrics.addMetric("genre_" + cleanGenre + "_ratingStdDev", row.getDouble(5));
                            } else {
                                metrics.addMetric("genre_" + cleanGenre + "_ratingStdDev", 0.0);
                            }
                        }
                    }
                } catch (Exception e) {
                    log.warn("Error processing genre row: {}", e.getMessage());
                }
            });

            // Calculate genre diversity metrics
            long totalGenres = genreStats.count();
            metrics.addMetric("totalGenres", totalGenres);
            metrics.addMetric("genreDataAvailable", true);

            // Find most and least popular genres (only if we have data)
            if (totalGenres > 0) {
                try {
                    Row mostPopular = genreStats.first();
                    Row leastPopular = genreStats.orderBy(functions.asc("ratingCount")).first();

                    metrics.addMetric("mostPopularGenre", mostPopular.getString(0));
                    metrics.addMetric("mostPopularGenreCount", mostPopular.getLong(1));
                    metrics.addMetric("leastPopularGenre", leastPopular.getString(0));
                    metrics.addMetric("leastPopularGenreCount", leastPopular.getLong(1));

                    // Calculate genre rating quality
                    Row bestRated = genreStats.orderBy(functions.desc("avgRating")).first();
                    Row worstRated = genreStats.orderBy(functions.asc("avgRating")).first();

                    metrics.addMetric("bestRatedGenre", bestRated.getString(0));
                    metrics.addMetric("bestRatedGenreAvg", bestRated.getDouble(2));
                    metrics.addMetric("worstRatedGenre", worstRated.getString(0));
                    metrics.addMetric("worstRatedGenreAvg", worstRated.getDouble(2));
                } catch (Exception e) {
                    log.warn("Error calculating genre rankings: {}", e.getMessage());
                    metrics.addMetric("genreRankingError", e.getMessage());
                }
            }

        } else {
            log.warn("Movies metadata not available - using fallback genre analysis");

            // Fallback implementation when movies data is not available
            metrics.addMetric("genreDataAvailable", false);
            metrics.addMetric("note", "Movies metadata not available - genre analysis limited");

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

            metrics.addMetric("totalMovies", movieDistribution.getLong(0));
            metrics.addMetric("avgRatingsPerMovie", movieDistribution.getDouble(1));
            metrics.addMetric("avgMovieRating", movieDistribution.getDouble(2));
        }
        analytics.add(new DataAnalytics(
                "content_analytics_" + UUID.randomUUID().toString().substring(0, 8),
                Instant.now(),
                AnalyticsType.GENRE_DISTRIBUTION,
                metrics.build(),
                "Comprehensive content analytics including  genres"
        ));
        log.debug("Genre distribution metrics collected");

    }

    /**
     * Collects content performance metrics.
     */
    private void collectContentPerformanceMetrics(Dataset<Row> ratingsDf,
                                                  AnalyticsMetrics metrics) {
        metrics.clearMetrics();
        log.debug("Collecting content performance metrics...");

        // Analyze movie performance metrics
        Dataset<Row> moviePerformance = ratingsDf.groupBy("movieId")
                .agg(
                        functions.count("ratingActual").as("totalRatings"),
                        functions.avg("ratingActual").as("avgRating"),
                        functions.stddev("ratingActual").as("ratingStdDev"),
                        functions.countDistinct("userId").as("uniqueRaters"),
                        functions.min("ratingActual").as("minRating"),
                        functions.max("ratingActual").as("maxRating")
                );

        // Calculate performance statistics
        Row performanceStats = moviePerformance.agg(
                functions.count("movieId").as("totalMovies"),
                functions.avg("totalRatings").as("avgRatingsPerMovie"),
                functions.avg("avgRating").as("overallAvgRating"),
                functions.avg("ratingStdDev").as("avgRatingVariability"),
                functions.avg("uniqueRaters").as("avgUniqueRatersPerMovie")
        ).first();

        metrics.addMetric("performance_totalMovies", performanceStats.getLong(0))
                .addMetric("performance_avgRatingsPerMovie", performanceStats.getDouble(1))
                .addMetric("performance_overallAvgRating", performanceStats.getDouble(2))
                .addMetricWithDefault("performance_avgRatingVariability", performanceStats.get(3), 0.0)
                .addMetric("performance_avgUniqueRatersPerMovie", performanceStats.getDouble(4));

        // Calculate content diversity metrics
        long highRatedMovies = moviePerformance.filter(functions.col("avgRating").gt(4.0)).count();
        long lowRatedMovies = moviePerformance.filter(functions.col("avgRating").lt(2.5)).count();
        long polarizingMovies = moviePerformance.filter(functions.col("ratingStdDev").gt(1.5)).count();

        metrics.addMetric("performance_highRatedMovies", highRatedMovies)
                .addMetric("performance_lowRatedMovies", lowRatedMovies)
                .addMetric("performance_polarizingMovies", polarizingMovies);
        analytics.add(new DataAnalytics(
                "content_analytics_" + UUID.randomUUID().toString().substring(0, 8),
                Instant.now(),
                AnalyticsType.CONTENT_PERFORMANCE,
                metrics.build(),
                "Comprehensive content analytics including and performance"
        ));
        log.debug("Content performance metrics collected");
    }

    @Override
    public String getAnalyticsType() {
        return "CONTENT_ANALYTICS";
    }

    @Override
    public boolean canProcess(Dataset<Row> ratingsDf, Dataset<Row> moviesData, Dataset<Row> tagsData) {
        return ratingsDf != null && !ratingsDf.isEmpty();
    }

    @Override
    public int getPriority() {
        return 20; // Medium-high priority for content analytics
    }
}
