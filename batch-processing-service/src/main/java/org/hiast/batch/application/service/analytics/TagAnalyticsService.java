package org.hiast.batch.application.service.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.apache.spark.storage.StorageLevel;
// import org.hiast.batch.application.pipeline.BasePipelineContext; // Not used in the provided method signature
import org.hiast.batch.domain.exception.AnalyticsCollectionException;
import org.hiast.model.AnalyticsType;
import org.hiast.model.DataAnalytics;
import org.hiast.model.analytics.AnalyticsMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.collection.JavaConverters;

import java.time.Instant;
import java.util.ArrayList; // Import ArrayList
import java.util.List;
import java.util.UUID;

/**
 * Service responsible for collecting user perspective tag analytics.
 * Analyzes how users perceive and describe movies through their tags,
 * providing insights into user perspectives and content perception.
 *
 * This service provides comprehensive tag analysis including sentiment,
 * genre correlation, and user perspective insights.
 * OPTIMIZED VERSION: Robust numeric getters, column pruning, and broadcast joins.
 */
public class TagAnalyticsService implements AnalyticsCollector {

    private static final Logger log = LoggerFactory.getLogger(TagAnalyticsService.class);

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
            throw new AnalyticsCollectionException("TAG_ANALYTICS", "Insufficient data for tag analytics");
        }



        List<DataAnalytics> collectedAnalytics = new ArrayList<>(); // Use a new list for this execution

        try {
            log.info("Collecting user perspective tag analytics (Optimized)...");
            AnalyticsMetrics metrics = AnalyticsMetrics.builder();

            if (tagsData == null || tagsData.isEmpty()) {
                log.warn("Tags data not available - user perspective analytics will be limited");
                metrics.addMetric("tagDataAvailable", false)
                        .addMetric("note", "Tags data not available for user perspective analysis");
            } else {
                // Persist tagsData if it's going to be used multiple times and isn't already cached
                // boolean tagsDataWasCached = tagsData.storageLevel().useMemory();
                // if (!tagsDataWasCached) {
                //     tagsData.persist(StorageLevel.MEMORY_AND_DISK_SER());
                //     log.debug("Persisted tagsData for TagAnalyticsService.");
                // }

                log.info("Analyzing user perspectives through tags...");
                metrics.addMetric("tagDataAvailable", true);

                collectBasicTagStatistics(tagsData, metrics);
                collectPopularTags(tagsData, metrics);
                collectTagSentimentAnalysis(tagsData, metrics);
                collectGenreTagAnalysis(tagsData, moviesData, metrics); // moviesData can be null here
                collectTagRatingCorrelation(tagsData, ratingsDf, metrics);

                // if (!tagsDataWasCached) {
                //     tagsData.unpersist();
                //     log.debug("Unpersisted tagsData in TagAnalyticsService.");
                // }
            }

            log.info("User perspective tag analytics collection completed with {} metrics", metrics.size());

            collectedAnalytics.add(new DataAnalytics(
                    "user_perspective_tags_" + UUID.randomUUID().toString().substring(0, 8),
                    Instant.now(),
                    AnalyticsType.USER_ENGAGEMENT, // As per original, though TAG_ANALYTICS might be more fitting if enum exists
                    metrics.build(),
                    "User perspective analytics through tags showing how users perceive and describe movies"
            ));
            return collectedAnalytics;

        } catch (Exception e) {
            log.error("Error collecting user perspective tag analytics: {}", e.getMessage(), e);
            throw new AnalyticsCollectionException("TAG_ANALYTICS", e.getMessage(), e);
        }
    }

    private void collectBasicTagStatistics(Dataset<Row> tagsData, AnalyticsMetrics metrics) {
        log.debug("Collecting basic tag statistics...");
        // These are full scans, ensure tagsData is cached if used multiple times before this.
        long totalTags = tagsData.count();
        metrics.addMetric("totalTags", totalTags);

        if (totalTags > 0) {
            long uniqueTags = tagsData.select("tag").distinct().count();
            long uniqueTaggedMovies = tagsData.select("movieId").distinct().count();
            long uniqueTaggingUsers = tagsData.select("userId").distinct().count();

            metrics.addMetric("uniqueTags", uniqueTags)
                    .addMetric("uniqueTaggedMovies", uniqueTaggedMovies)
                    .addMetric("uniqueTaggingUsers", uniqueTaggingUsers);

            if (uniqueTaggedMovies > 0) {
                metrics.addMetric("avgTagsPerMovie", (double) totalTags / uniqueTaggedMovies);
            } else {
                metrics.addMetric("avgTagsPerMovie", 0.0);
            }
            if (uniqueTaggingUsers > 0) {
                metrics.addMetric("avgTagsPerUser", (double) totalTags / uniqueTaggingUsers);
            } else {
                metrics.addMetric("avgTagsPerUser", 0.0);
            }
        } else {
            metrics.addMetric("uniqueTags", 0L)
                    .addMetric("uniqueTaggedMovies", 0L)
                    .addMetric("uniqueTaggingUsers", 0L)
                    .addMetric("avgTagsPerMovie", 0.0)
                    .addMetric("avgTagsPerUser", 0.0);
        }
        log.debug("Basic tag statistics collected");
    }

    private void collectPopularTags(Dataset<Row> tagsData, AnalyticsMetrics metrics) {
        log.debug("Collecting popular tags...");
        // Select only necessary columns for this aggregation
        Dataset<Row> relevantTagsForPopularity = tagsData.select("tag", "movieId", "userId");

        Dataset<Row> popularTags = relevantTagsForPopularity
                .groupBy("tag")
                .agg(
                        functions.count("tag").as("tagCount"), // Count occurrences of the tag
                        functions.countDistinct("movieId").as("moviesTagged"),
                        functions.countDistinct("userId").as("usersWhoTagged")
                )
                .orderBy(functions.desc("tagCount"))
                .limit(20); // This result is small

        // .collectAsList() on 20 rows is fine.
        List<Row> collectedPopularTags = popularTags.collectAsList();
        for(Row row : collectedPopularTags) {
            String tag = row.getString(row.fieldIndex("tag"));
            long tagCount = getLongFromRow(row, "tagCount", 0L);
            long moviesTagged = getLongFromRow(row, "moviesTagged", 0L);
            long usersWhoTagged = getLongFromRow(row, "usersWhoTagged", 0L);

            String safeTag = tag != null ? tag.replaceAll("[^a-zA-Z0-9]", "").toLowerCase() : "nulltag";
            if (safeTag.isEmpty()) safeTag = "emptyTag"; // Ensure key is not empty

            metrics.addMetric("popularTag_" + safeTag + "_name", tag)
                    .addMetric("popularTag_" + safeTag + "_count", tagCount)
                    .addMetric("popularTag_" + safeTag + "_moviesTagged", moviesTagged)
                    .addMetric("popularTag_" + safeTag + "_usersWhoTagged", usersWhoTagged);
        }
        log.debug("Popular tags collected");
    }

    private void collectTagSentimentAnalysis(Dataset<Row> tagsData, AnalyticsMetrics metrics) {
        log.debug("Collecting tag sentiment analysis...");
        String[] positiveWords = {"good", "great", "excellent", "amazing", "awesome", "fantastic", "wonderful", "brilliant", "outstanding", "perfect"};
        String[] negativeWords = {"bad", "terrible", "awful", "horrible", "worst", "boring", "stupid", "disappointing", "waste", "sucks"};

        long positiveTags = 0;
        long negativeTags = 0;

        // Select only the 'tag' column for filtering
        Dataset<Row> tagColumnOnly = tagsData.select(functions.lower(functions.col("tag")).as("lower_tag"));
        tagColumnOnly.persist(StorageLevel.MEMORY_AND_DISK_SER()); // Cache for multiple filters

        for (String word : positiveWords) {
            positiveTags += tagColumnOnly.filter(functions.col("lower_tag").contains(word)).count();
        }
        for (String word : negativeWords) {
            negativeTags += tagColumnOnly.filter(functions.col("lower_tag").contains(word)).count();
        }
        tagColumnOnly.unpersist();

        metrics.addMetric("positiveTags", positiveTags)
                .addMetric("negativeTags", negativeTags);
        if (negativeTags > 0) {
            metrics.addMetric("sentimentRatio", (double) positiveTags / negativeTags);
        } else if (positiveTags > 0) {
            metrics.addMetric("sentimentRatio", Double.POSITIVE_INFINITY); // Or a large number
        } else {
            metrics.addMetric("sentimentRatio", 0.0);
        }
        log.debug("Tag sentiment analysis collected");
    }

    private void collectGenreTagAnalysis(Dataset<Row> tagsData, Dataset<Row> moviesData, AnalyticsMetrics metrics) {
        log.debug("Collecting genre-based tag analysis...");
        if (moviesData == null || moviesData.isEmpty()) {
            metrics.addMetric("genreTagAnalysisAvailable", false);
            log.warn("Movies data not available for genre-tag analysis.");
            return;
        }
        log.debug("Analyzing tags by genre...");

        // --- OPTIMIZATION: Select only necessary columns before join and broadcast ---
        Dataset<Row> relevantTagsForGenreAnalysis = tagsData.select("movieId", "tag");
        Dataset<Row> relevantMoviesForGenreAnalysis = moviesData.select("movieId", "genres");

        Dataset<Row> tagsWithGenres = relevantTagsForGenreAnalysis
                .join(functions.broadcast(relevantMoviesForGenreAnalysis), // Broadcast smaller moviesData
                        relevantTagsForGenreAnalysis.col("movieId").equalTo(relevantMoviesForGenreAnalysis.col("movieId")), "inner")
                .select(
                        relevantTagsForGenreAnalysis.col("tag"),
                        functions.explode(functions.split(relevantMoviesForGenreAnalysis.col("genres"), "\\|")).as("genre")
                )
                .filter(functions.col("genre").notEqual("(no genres listed)"))
                .filter(functions.col("genre").isNotNull())
                .filter(functions.length(functions.col("genre")).gt(0));

        // This DataFrame can be large after explode, persist it before window function
        tagsWithGenres.persist(StorageLevel.MEMORY_AND_DISK_SER());
        log.debug("Persisted tagsWithGenres for genre-tag analysis. Count: {}", tagsWithGenres.count());


        Dataset<Row> genreTagAnalysis = tagsWithGenres
                .groupBy("genre", "tag")
                .count() // This count is the number of times a tag appears for a genre
                .withColumnRenamed("count", "tagCount")
                .withColumn("rank", functions.row_number().over(
                        org.apache.spark.sql.expressions.Window
                                .partitionBy("genre")
                                .orderBy(functions.desc("tagCount"))
                ))
                .filter(functions.col("rank").leq(3)); // Top 3 tags per genre

        // This result (genreTagAnalysis) should be small (num_genres * 3)
        List<Row> collectedGenreTags = genreTagAnalysis.collectAsList();
        tagsWithGenres.unpersist(); // Unpersist intermediate DataFrame
        log.debug("Unpersisted tagsWithGenres.");

        for(Row row : collectedGenreTags) {
            String genre = row.getString(row.fieldIndex("genre"));
            String tag = row.getString(row.fieldIndex("tag"));
            long tagCount = getLongFromRow(row, "tagCount", 0L);
            int rank = getIntFromRow(row, "rank", 0);

            String safeGenre = genre != null ? genre.replaceAll("[^a-zA-Z0-9]", "").toLowerCase() : "nullgenre";
            String safeTag = tag != null ? tag.replaceAll("[^a-zA-Z0-9]", "").toLowerCase() : "nulltag";
            if (safeGenre.isEmpty()) safeGenre = "emptyGenre";
            if (safeTag.isEmpty()) safeTag = "emptyTag";

            metrics.addMetric("genre_" + safeGenre + "_topTag" + rank + "_name", tag)
                    .addMetric("genre_" + safeGenre + "_topTag" + rank + "_count", tagCount);
        }
        metrics.addMetric("genreTagAnalysisAvailable", true);
        log.debug("Genre-based tag analysis collected");
    }

    private void collectTagRatingCorrelation(Dataset<Row> tagsData, Dataset<Row> ratingsDf, AnalyticsMetrics metrics) {
        log.debug("Collecting tag-rating correlation...");

        // --- OPTIMIZATION: Select only necessary columns before join ---
        Dataset<Row> relevantTagsForCorrelation = tagsData.select(
                functions.col("movieId"),
                functions.col("userId"),
                functions.col("tag")
        );
        Dataset<Row> relevantRatingsForCorrelation = ratingsDf.select(
                functions.col("movieId"),
                functions.col("userId"),
                functions.col("ratingActual")
        );

        // Determine which DataFrame is smaller for potential broadcast.
        // For MovieLens, tagsData is often smaller than ratingsDf.
        // If tagsData is significantly smaller, broadcast it.
        // This assumes tagsData is smaller. If not, this strategy might need adjustment or rely on AQE.
        Dataset<Row> tagsWithRatings = relevantRatingsForCorrelation
                .join(functions.broadcast(relevantTagsForCorrelation), // Broadcast pruned tagsData
                        JavaConverters.asScalaBufferConverter(java.util.Arrays.asList("movieId", "userId")).asScala().toSeq(), // Join on multiple columns
                        "inner")
                .select(
                        relevantTagsForCorrelation.col("tag"),
                        relevantRatingsForCorrelation.col("ratingActual")
                );

        // Persist if tagsWithRatings is large and used multiple times, though here it's used once for aggregation.
        // tagsWithRatings.persist(StorageLevel.MEMORY_AND_DISK_SER());
        // log.debug("Persisted tagsWithRatings for correlation. Count: {}", tagsWithRatings.count());


        if (tagsWithRatings.isEmpty()) { // Check if join resulted in empty DF
            metrics.addMetric("tagRatingCorrelationAvailable", false);
            log.warn("No correlation data between tags and ratings after join.");
            // if (tagsWithRatings.storageLevel().useMemory()) tagsWithRatings.unpersist();
            return;
        }

        Dataset<Row> tagRatingCorrelation = tagsWithRatings
                .groupBy("tag")
                .agg(
                        functions.avg("ratingActual").as("avgRating"),
                        functions.count("ratingActual").as("ratingCount")
                )
                .filter(functions.col("ratingCount").gt(5))
                .orderBy(functions.desc("avgRating"))
                .limit(10); // Result is small

        // if (tagsWithRatings.storageLevel().useMemory()) tagsWithRatings.unpersist();
        // log.debug("Unpersisted tagsWithRatings.");

        List<Row> collectedTagRatings = tagRatingCorrelation.collectAsList();
        if (collectedTagRatings.isEmpty()) {
            metrics.addMetric("tagRatingCorrelationAvailable", false);
            log.warn("No tag-rating correlation data met the filter criteria (count > 5).");
            return;
        }


        for(Row row : collectedTagRatings) {
            String tag = row.getString(row.fieldIndex("tag"));
            double avgRating = getDoubleFromRow(row, "avgRating", 0.0);
            long ratingCount = getLongFromRow(row, "ratingCount", 0L);

            String safeTag = tag != null ? tag.replaceAll("[^a-zA-Z0-9]", "").toLowerCase() : "nulltag";
            if (safeTag.isEmpty()) safeTag = "emptyTag";

            metrics.addMetric("highRatedTag_" + safeTag + "_name", tag)
                    .addMetric("highRatedTag_" + safeTag + "_avgRating", avgRating)
                    .addMetric("highRatedTag_" + safeTag + "_ratingCount", ratingCount);
        }
        metrics.addMetric("tagRatingCorrelationAvailable", true);
        log.debug("Tag-rating correlation collected");
    }

    @Override
    public String getAnalyticsType() {
        return "TAG_ANALYTICS"; // Consider defining AnalyticsType.TAG_ANALYTICS
    }

    @Override
    public boolean canProcess(Dataset<Row> ratingsDf, Dataset<Row> moviesData, Dataset<Row> tagsData) {
        // This service heavily relies on tagsData for most of its metrics.
        // While some basic stats could be run on ratingsDf, the core value is from tags.
        // The original code had a check for tagsData within collectAnalytics.
        // For canProcess, it might be better to require tagsData.
        return ratingsDf != null && !ratingsDf.isEmpty() && tagsData != null && !tagsData.isEmpty();
    }

    @Override
    public int getPriority() {
        return 50;
    }
}
