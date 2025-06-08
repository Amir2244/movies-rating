package org.hiast.batch.application.service.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.functions;
import org.apache.spark.storage.StorageLevel;
import org.hiast.batch.domain.exception.AnalyticsCollectionException;
import org.hiast.model.AnalyticsType;
import org.hiast.model.DataAnalytics;
import org.hiast.model.analytics.AnalyticsMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Service responsible for collecting content-related analytics.
 * Handles movie popularity, genre distribution, and content performance analytics.
 * <p>
 * This service focuses on content analysis including movie performance,
 * genre preferences, and content diversity metrics.
 * CORRECTED VERSION V3: Implements robust numeric retrieval from Row objects
 * to prevent ClassCastExceptions.
 */
public class ContentAnalyticsService implements AnalyticsCollector {

    private static final Logger log = LoggerFactory.getLogger(ContentAnalyticsService.class);

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
            } else {
                // Fallback for string representation, though less ideal if schema is known
                return Double.parseDouble(value.toString());
            }
        } catch (Exception e) { // Catch broader exceptions like fieldIndex not found or parsing
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
            } else {
                return Long.parseLong(value.toString());
            }
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
            throw new AnalyticsCollectionException("CONTENT_ANALYTICS", "Insufficient data for content analytics");
        }

        List<DataAnalytics> results = new ArrayList<>();
        SparkSession spark = ratingsDf.sparkSession();

        try {
            log.info("Collecting content analytics (Corrected Version v3)...");

            collectMoviePopularityMetrics(ratingsDf, moviesData, tagsData, results);
            collectGenreDistributionMetrics(ratingsDf, moviesData, spark, results);
            collectContentPerformanceMetrics(ratingsDf, results);

            log.info("Content analytics collection completed with {} analytics objects.", results.size());
            return results;

        } catch (Exception e) {
            log.error("Error collecting content analytics: {}", e.getMessage(), e);
            throw new AnalyticsCollectionException("CONTENT_ANALYTICS", e.getMessage(), e);
        }
    }

    private void collectMoviePopularityMetrics(Dataset<Row> ratingsDf, Dataset<Row> moviesData,
                                               Dataset<Row> tagsData, List<DataAnalytics> results) {
        log.debug("Collecting movie popularity metrics...");
        AnalyticsMetrics metrics = AnalyticsMetrics.builder();

        Dataset<Row> relevantRatingsForPopularity = ratingsDf.select(
                functions.col("movieId"),
                functions.col("ratingActual")
        );

        Dataset<Row> moviePopularity = relevantRatingsForPopularity.groupBy("movieId")
                .agg(
                        functions.count("ratingActual").as("ratingsCount"), // LongType
                        functions.avg("ratingActual").as("avgRating"),     // DoubleType
                        functions.stddev_samp("ratingActual").as("ratingStdDev") // DoubleType
                );

        Dataset<Row> moviePopularityForStats = moviePopularity;

        if (moviesData != null && !moviesData.isEmpty()) {
            Dataset<Row> relevantMoviesData = moviesData.select(
                    functions.col("movieId"),
                    functions.col("title"),
                    functions.col("genres")
            );
            moviePopularityForStats = moviePopularity
                    .join(functions.broadcast(relevantMoviesData), moviePopularity.col("movieId").equalTo(relevantMoviesData.col("movieId")), "left_outer")
                    .select(
                            moviePopularity.col("movieId"),
                            relevantMoviesData.col("title"),
                            relevantMoviesData.col("genres"),
                            moviePopularity.col("ratingsCount"),
                            moviePopularity.col("avgRating"),
                            moviePopularity.col("ratingStdDev")
                    );
        }

        Row movieStats = moviePopularityForStats.agg(
                functions.count(moviePopularityForStats.col("movieId")).as("totalMovies"), // Long
                functions.avg("ratingsCount").as("avgRatingsPerMovie"), // Double (avg of Long)
                functions.min("ratingsCount").as("minRatingsPerMovie"), // Long
                functions.max("ratingsCount").as("maxRatingsPerMovie"), // Long
                functions.expr("percentile_approx(ratingsCount, 0.5)").as("medianRatingsPerMovie"), // Double
                functions.avg("avgRating").as("overallAvgRating") // Double (avg of Double)
        ).first();

        if (movieStats != null) {
            metrics.addMetric("popularity_totalMovies", getLongFromRow(movieStats, "totalMovies", 0L))
                    .addMetric("popularity_avgRatingsPerMovie", getDoubleFromRow(movieStats, "avgRatingsPerMovie", 0.0))
                    .addMetric("popularity_minRatingsPerMovie", getLongFromRow(movieStats, "minRatingsPerMovie", 0L))
                    .addMetric("popularity_maxRatingsPerMovie", getLongFromRow(movieStats, "maxRatingsPerMovie", 0L))
                    .addMetric("popularity_medianRatingsPerMovie", getDoubleFromRow(movieStats, "medianRatingsPerMovie", 0.0))
                    .addMetric("popularity_overallAvgRating", getDoubleFromRow(movieStats, "overallAvgRating", 0.0)); // This was line ~131
        }

        Dataset<Row> topMovies = moviePopularityForStats.orderBy(functions.desc("ratingsCount")).limit(10);
        List<Row> collectedTopMovies = topMovies.collectAsList();

        for (Row row : collectedTopMovies) {
            long movieId = getLongFromRow(row, "movieId", 0L); // movieId is int
            long ratingsCount = getLongFromRow(row, "ratingsCount", 0L);

            if (moviesData != null && !moviesData.isEmpty() && !row.isNullAt(row.fieldIndex("title"))) {
                String title = row.getString(row.fieldIndex("title"));
                String genres = row.isNullAt(row.fieldIndex("genres")) ? "(no genres listed)" : row.getString(row.fieldIndex("genres"));
                double avgRating = getDoubleFromRow(row, "avgRating", 0.0);

                metrics.addMetric("topMovie_" + movieId + "_title", title)
                        .addMetric("topMovie_" + movieId + "_genres", genres)
                        .addMetric("topMovie_" + movieId + "_ratingsCount", ratingsCount)
                        .addMetric("topMovie_" + movieId + "_avgRating", avgRating);
            } else {
                metrics.addMetric("topMovie_" + movieId + "_ratingsCount", ratingsCount);
            }
        }

        if (tagsData != null && !tagsData.isEmpty()) {
            addTagInsights(topMovies.select("movieId"), tagsData, metrics);
        }
        results.add(new DataAnalytics(
                "content_analytics_popularity_" + UUID.randomUUID().toString().substring(0, 8),
                Instant.now(),
                AnalyticsType.MOVIE_POPULARITY,
                metrics.build(),
                "Movie popularity analytics including top movies and tag insights"
        ));
        log.debug("Movie popularity metrics collected");
    }

    private void addTagInsights(Dataset<Row> topMovieIdsDf, Dataset<Row> tagsData, AnalyticsMetrics metrics) {
        log.debug("Adding tag insights for popular movies...");
        try {
            Dataset<Row> relevantTagsData = tagsData.select(
                    functions.col("movieId"),
                    functions.col("tag")
            );

            Dataset<Row> popularMovieTags = functions.broadcast(topMovieIdsDf)
                    .join(relevantTagsData, topMovieIdsDf.col("movieId").equalTo(relevantTagsData.col("movieId")), "inner")
                    .groupBy(topMovieIdsDf.col("movieId"), relevantTagsData.col("tag"))
                    .count()
                    .withColumnRenamed("count", "tagCount");

            Dataset<Row> topTagsPerMovie = popularMovieTags
                    .withColumn("rank", functions.row_number().over(
                            org.apache.spark.sql.expressions.Window
                                    .partitionBy(popularMovieTags.col("movieId"))
                                    .orderBy(functions.desc("tagCount"))
                    ))
                    .filter(functions.col("rank").leq(3));

            List<Row> collectedTopTags = topTagsPerMovie.collectAsList();
            for (Row row : collectedTopTags) {
                long movieId = getLongFromRow(row, "movieId", 0L);
                String tag = row.getString(row.fieldIndex("tag"));
                long tagCount = getLongFromRow(row, "tagCount", 0L);
                long rank = getLongFromRow(row, "rank", 0L); // rank is int

                metrics.addMetric("topMovie_" + movieId + "_tag" + rank + "_name", tag)
                        .addMetric("topMovie_" + movieId + "_tag" + rank + "_count", tagCount);
            }
            metrics.addMetric("tagAnalysisAvailable", true);
        } catch (Exception e) {
            log.warn("Error adding tag insights: {}", e.getMessage(), e);
            metrics.addMetric("tagAnalysisAvailable", false);
        }
    }

    private void collectGenreDistributionMetrics(Dataset<Row> ratingsDf, Dataset<Row> moviesData, SparkSession spark, List<DataAnalytics> results) {
        AnalyticsMetrics metrics = AnalyticsMetrics.builder();
        log.debug("Collecting genre distribution metrics (Corrected v3)...");

        if (moviesData == null || moviesData.isEmpty()) {
            log.warn("Movies metadata not available - using fallback genre analysis for genre distribution");
            metrics.addMetric("genreDataAvailable", false);
            metrics.addMetric("note", "Movies metadata not available - genre analysis limited");

            Dataset<Row> movieStatsFallback = ratingsDf.groupBy("movieId")
                    .agg(
                            functions.count("ratingActual").as("ratingCount"),
                            functions.avg("ratingActual").as("avgRating")
                    );
            Row movieDistributionFallback = movieStatsFallback.agg(
                    functions.count("movieId").as("totalMovies"),
                    functions.avg("ratingCount").as("avgRatingsPerMovie"),
                    functions.avg("avgRating").as("avgMovieRating")
            ).first();

            if (movieDistributionFallback != null) {
                metrics.addMetric("totalMovies", getLongFromRow(movieDistributionFallback, "totalMovies", 0L));
                metrics.addMetric("avgRatingsPerMovie", getDoubleFromRow(movieDistributionFallback, "avgRatingsPerMovie", 0.0));
                metrics.addMetric("avgMovieRating", getDoubleFromRow(movieDistributionFallback, "avgMovieRating", 0.0));
            }
            results.add(new DataAnalytics(
                    "content_analytics_genre_fallback_" + UUID.randomUUID().toString().substring(0, 8),
                    Instant.now(),
                    AnalyticsType.GENRE_DISTRIBUTION,
                    metrics.build(),
                    "Genre distribution analytics (fallback due to missing movie data)"
            ));
            log.debug("Fallback genre distribution metrics collected.");
            return;
        }

        log.info("Using actual movie metadata for genre analysis (Corrected v3)");

        Dataset<Row> relevantRatingsForGenre = ratingsDf.select(
                functions.col("movieId"),
                functions.col("userId"),
                functions.col("ratingActual")
        );
        Dataset<Row> relevantMoviesForGenre = moviesData.select(
                functions.col("movieId"),
                functions.col("genres")
        );

        Dataset<Row> ratingsWithGenres = relevantRatingsForGenre
                .join(functions.broadcast(relevantMoviesForGenre),
                        relevantRatingsForGenre.col("movieId").equalTo(relevantMoviesForGenre.col("movieId")), "inner")
                .select(
                        relevantRatingsForGenre.col("userId"),
                        relevantRatingsForGenre.col("ratingActual"),
                        relevantRatingsForGenre.col("movieId"),
                        relevantMoviesForGenre.col("genres")
                );

        Dataset<Row> genreRatings = ratingsWithGenres
                .withColumn("genre", functions.explode(functions.split(functions.col("genres"), "\\|")))
                .filter(functions.col("genre").notEqual("(no genres listed)"))
                .filter(functions.col("genre").isNotNull())
                .filter(functions.length(functions.col("genre")).gt(0));

        genreRatings.persist(StorageLevel.MEMORY_AND_DISK_SER());
        long genreRatingsCount = genreRatings.count();
        log.info("Count of genreRatings (after explode and persist): {}", genreRatingsCount);

        int numShufflePartitions = spark.sessionState().conf().numShufflePartitions();
        Dataset<Row> repartitionedGenreRatings = genreRatings.repartition(numShufflePartitions, functions.col("genre"));

        Dataset<Row> genreStats = repartitionedGenreRatings.groupBy("genre")
                .agg(
                        functions.count("ratingActual").as("ratingCount"),
                        functions.avg("ratingActual").as("avgRating"),
                        functions.countDistinct("userId").as("uniqueUsers"),
                        functions.countDistinct("movieId").as("uniqueMovies"),
                        functions.stddev_samp("ratingActual").as("ratingStdDev")
                )
                .orderBy(functions.desc("ratingCount"));

        long uniqueGenreCount = genreStats.count();
        log.info("Count of unique genres (genreStats): {}", uniqueGenreCount);
        if (uniqueGenreCount > 500) {
            log.warn("WARNING: Number of unique genres ({}) is very high. This might impact collectAsList performance.", uniqueGenreCount);
        }

        List<Row> collectedGenreStats = genreStats.collectAsList();
        long totalGenres = collectedGenreStats.size();
        log.debug("Collected genreStats to driver ({} genres).", totalGenres);

        genreRatings.unpersist();

        for (Row row : collectedGenreStats) {
            try {
                String genre = row.getString(row.fieldIndex("genre"));
                if (genre != null && !genre.trim().isEmpty()) {
                    String cleanGenre = genre.replaceAll("[^a-zA-Z0-9]", "").toLowerCase();
                    if (!cleanGenre.isEmpty()) {
                        metrics.addMetric("genre_" + cleanGenre + "_name", genre);
                        metrics.addMetric("genre_" + cleanGenre + "_count", getLongFromRow(row, "ratingCount", 0L));
                        metrics.addMetric("genre_" + cleanGenre + "_avgRating", getDoubleFromRow(row, "avgRating", 0.0));
                        metrics.addMetric("genre_" + cleanGenre + "_uniqueUsers", getLongFromRow(row, "uniqueUsers", 0L));
                        metrics.addMetric("genre_" + cleanGenre + "_uniqueMovies", getLongFromRow(row, "uniqueMovies", 0L));
                        metrics.addMetricWithDefault("genre_" + cleanGenre + "_ratingStdDev", row.get(row.fieldIndex("ratingStdDev")), 0.0); // Using addMetricWithDefault for safety
                    }
                }
            } catch (Exception e) {
                log.warn("Error processing genre row: {}", e.getMessage());
            }
        }

        metrics.addMetric("totalGenres", totalGenres);
        metrics.addMetric("genreDataAvailable", true);

        if (totalGenres > 0) {
            try {
                Row mostPopular = genreStats.orderBy(functions.desc("ratingCount")).first();
                Row leastPopular = genreStats.orderBy(functions.asc("ratingCount")).first();

                if (mostPopular != null) {
                    metrics.addMetric("mostPopularGenre", mostPopular.getString(mostPopular.fieldIndex("genre")));
                    metrics.addMetric("mostPopularGenreCount", getLongFromRow(mostPopular, "ratingCount", 0L));
                }
                if (leastPopular != null) {
                    metrics.addMetric("leastPopularGenre", leastPopular.getString(leastPopular.fieldIndex("genre")));
                    metrics.addMetric("leastPopularGenreCount", getLongFromRow(leastPopular, "ratingCount", 0L));
                }

                Row bestRated = genreStats.orderBy(functions.desc("avgRating")).first();
                Row worstRated = genreStats.orderBy(functions.asc("avgRating")).first();

                if (bestRated != null) {
                    metrics.addMetric("bestRatedGenre", bestRated.getString(bestRated.fieldIndex("genre")));
                    metrics.addMetric("bestRatedGenreAvg", getDoubleFromRow(bestRated, "avgRating", 0.0));
                }
                if (worstRated != null) {
                    metrics.addMetric("worstRatedGenre", worstRated.getString(worstRated.fieldIndex("genre")));
                    metrics.addMetric("worstRatedGenreAvg", getDoubleFromRow(worstRated, "avgRating", 0.0));
                }
            } catch (Exception e) {
                log.warn("Error calculating genre rankings: {}", e.getMessage());
                metrics.addMetric("genreRankingError", e.getMessage());
            }
        }

        results.add(new DataAnalytics(
                "content_analytics_genre_" + UUID.randomUUID().toString().substring(0, 8),
                Instant.now(),
                AnalyticsType.GENRE_DISTRIBUTION,
                metrics.build(),
                "Genre distribution and preference analytics"
        ));
        log.debug("Genre distribution metrics collected");
    }

    private void collectContentPerformanceMetrics(Dataset<Row> ratingsDf, List<DataAnalytics> results) {
        AnalyticsMetrics metrics = AnalyticsMetrics.builder();
        log.debug("Collecting content performance metrics...");

        Dataset<Row> relevantRatingsForPerformance = ratingsDf.select(
                functions.col("movieId"),
                functions.col("userId"),
                functions.col("ratingActual")
        );

        Dataset<Row> moviePerformance = relevantRatingsForPerformance.groupBy("movieId")
                .agg(
                        functions.count("ratingActual").as("totalRatings"),
                        functions.avg("ratingActual").as("avgRating"),
                        functions.stddev_samp("ratingActual").as("ratingStdDev"),
                        functions.countDistinct("userId").as("uniqueRaters"),
                        functions.min("ratingActual").as("minRating"),
                        functions.max("ratingActual").as("maxRating")
                );

        Row performanceStats = moviePerformance.agg(
                functions.count("movieId").as("totalMovies"),
                functions.avg("totalRatings").as("avgRatingsPerMovie"),
                functions.avg("avgRating").as("overallAvgRating"),
                functions.avg("ratingStdDev").as("avgRatingVariability"),
                functions.avg("uniqueRaters").as("avgUniqueRatersPerMovie")
        ).first();

        if (performanceStats != null) {
            metrics.addMetric("performance_totalMovies", getLongFromRow(performanceStats, "totalMovies", 0L))
                    .addMetric("performance_avgRatingsPerMovie", getDoubleFromRow(performanceStats, "avgRatingsPerMovie", 0.0))
                    .addMetric("performance_overallAvgRating", getDoubleFromRow(performanceStats, "overallAvgRating", 0.0))
                    .addMetricWithDefault("performance_avgRatingVariability", performanceStats.get(performanceStats.fieldIndex("avgRatingVariability")), 0.0)
                    .addMetric("performance_avgUniqueRatersPerMovie", getDoubleFromRow(performanceStats, "avgUniqueRatersPerMovie", 0.0));
        }

        long highRatedMovies = moviePerformance.filter(functions.col("avgRating").gt(4.0)).count();
        long lowRatedMovies = moviePerformance.filter(functions.col("avgRating").lt(2.5)).count();
        long polarizingMovies = moviePerformance.filter(functions.col("ratingStdDev").isNotNull().and(functions.col("ratingStdDev").gt(1.5))).count();

        metrics.addMetric("performance_highRatedMovies", highRatedMovies)
                .addMetric("performance_lowRatedMovies", lowRatedMovies)
                .addMetric("performance_polarizingMovies", polarizingMovies);

        results.add(new DataAnalytics(
                "content_analytics_performance_" + UUID.randomUUID().toString().substring(0, 8),
                Instant.now(),
                AnalyticsType.CONTENT_PERFORMANCE,
                metrics.build(),
                "Content performance analytics including ratings variability and polarization"
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
        return 20;
    }
}
