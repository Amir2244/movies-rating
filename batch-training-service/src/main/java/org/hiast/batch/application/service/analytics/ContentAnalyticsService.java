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
 * Service responsible for collecting content-related analytics.
 * Handles movie popularity, genre distribution, and content performance analytics.
 * 
 * This service focuses on content analysis including movie performance,
 * genre preferences, and content diversity metrics.
 */
public class ContentAnalyticsService implements AnalyticsCollector {
    
    private static final Logger log = LoggerFactory.getLogger(ContentAnalyticsService.class);
    
    @Override
    public DataAnalytics collectAnalytics(Dataset<Row> ratingsDf, 
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
            collectContentPerformanceMetrics(ratingsDf, moviesData, metrics);
            
            log.info("Content analytics collection completed with {} metrics", metrics.size());
            
            return new DataAnalytics(
                    "content_analytics_" + UUID.randomUUID().toString().substring(0, 8),
                    Instant.now(),
                    AnalyticsType.CONTENT_PERFORMANCE,
                    metrics.build(),
                    "Comprehensive content analytics including popularity, genres, and performance"
            );
            
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
                    .filter(functions.col("rank").leq(3)); // Top 3 tags per movie
            
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
        log.debug("Collecting genre distribution metrics...");
        
        try {
            // Join ratings with movies to get genre information
            Dataset<Row> ratingsWithGenres = ratingsDf
                    .join(moviesData, ratingsDf.col("movieId").equalTo(moviesData.col("movieId")), "inner")
                    .select(ratingsDf.col("*"), moviesData.col("genres"));

            // Split genres and create individual genre records
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
                        String cleanGenre = genre.replaceAll("[^a-zA-Z0-9]", "").toLowerCase();
                        if (!cleanGenre.isEmpty()) {
                            metrics.addMetric("genre_" + cleanGenre + "_name", genre)
                                   .addMetric("genre_" + cleanGenre + "_count", row.getLong(1))
                                   .addMetric("genre_" + cleanGenre + "_avgRating", row.getDouble(2))
                                   .addMetric("genre_" + cleanGenre + "_uniqueUsers", row.getLong(3))
                                   .addMetric("genre_" + cleanGenre + "_uniqueMovies", row.getLong(4))
                                   .addMetricWithDefault("genre_" + cleanGenre + "_ratingStdDev", row.get(5), 0.0);
                        }
                    }
                } catch (Exception e) {
                    log.warn("Error processing genre row: {}", e.getMessage());
                }
            });

            // Calculate genre diversity metrics
            long totalGenres = genreStats.count();
            metrics.addMetric("genre_totalGenres", totalGenres)
                   .addMetric("genre_dataAvailable", true);

            // Find most and least popular genres (only if we have data)
            if (totalGenres > 0) {
                try {
                    Row mostPopular = genreStats.first();
                    Row leastPopular = genreStats.orderBy(functions.asc("ratingCount")).first();
                    
                    metrics.addMetric("genre_mostPopular", mostPopular.getString(0))
                           .addMetric("genre_mostPopularCount", mostPopular.getLong(1))
                           .addMetric("genre_leastPopular", leastPopular.getString(0))
                           .addMetric("genre_leastPopularCount", leastPopular.getLong(1));
                } catch (Exception e) {
                    log.warn("Error calculating genre rankings: {}", e.getMessage());
                }
            }
            
        } catch (Exception e) {
            log.warn("Error collecting genre distribution metrics: {}", e.getMessage());
            metrics.addMetric("genre_dataAvailable", false);
        }
        
        log.debug("Genre distribution metrics collected");
    }
    
    /**
     * Collects content performance metrics.
     */
    private void collectContentPerformanceMetrics(Dataset<Row> ratingsDf, Dataset<Row> moviesData, 
                                                AnalyticsMetrics metrics) {
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
