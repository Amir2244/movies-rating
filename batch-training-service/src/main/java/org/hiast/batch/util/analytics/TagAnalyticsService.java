package org.hiast.batch.util.analytics;

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
 * Service responsible for collecting user perspective tag analytics.
 * Analyzes how users perceive and describe movies through their tags,
 * providing insights into user perspectives and content perception.
 * 
 * This service provides comprehensive tag analysis including sentiment,
 * genre correlation, and user perspective insights.
 */
public class TagAnalyticsService implements AnalyticsCollector {
    
    private static final Logger log = LoggerFactory.getLogger(TagAnalyticsService.class);
    
    @Override
    public List<DataAnalytics> collectAnalytics(Dataset<Row> ratingsDf,
                                                Dataset<Row> moviesData,
                                                Dataset<Row> tagsData,
                                                ALSTrainingPipelineContext context) {
        
        if (!canProcess(ratingsDf, moviesData, tagsData)) {
            throw new AnalyticsCollectionException("TAG_ANALYTICS", "Insufficient data for tag analytics");
        }
        
        try {
            log.info("Collecting user perspective tag analytics...");
            
            AnalyticsMetrics metrics = AnalyticsMetrics.builder();
            
            if (tagsData == null || tagsData.isEmpty()) {
                log.warn("Tags data not available - user perspective analytics will be limited");
                metrics.addMetric("tagDataAvailable", false)
                       .addMetric("note", "Tags data not available for user perspective analysis");
            } else {
                log.info("Analyzing user perspectives through tags...");
                metrics.addMetric("tagDataAvailable", true);

                // Collect all tag analytics - exact same as original
                collectBasicTagStatistics(tagsData, metrics);
                collectPopularTags(tagsData, metrics);
                collectTagSentimentAnalysis(tagsData, metrics);
                collectGenreTagAnalysis(tagsData, moviesData, metrics);
                collectTagRatingCorrelation(tagsData, ratingsDf, metrics);
            }
            
            log.info("User perspective tag analytics collection completed with {} metrics", metrics.size());
            
            return Collections.singletonList(new DataAnalytics(
                    "user_perspective_tags_" + UUID.randomUUID().toString().substring(0, 8),
                    Instant.now(),
                    AnalyticsType.USER_ENGAGEMENT, // Using existing type as in original
                    metrics.build(),
                    "User perspective analytics through tags showing how users perceive and describe movies"
            ));
            
        } catch (Exception e) {
            log.error("Error collecting user perspective tag analytics: {}", e.getMessage(), e);
            throw new AnalyticsCollectionException("TAG_ANALYTICS", e.getMessage(), e);
        }
    }
    
    /**
     * Collects basic tag statistics - exact same as original.
     */
    private void collectBasicTagStatistics(Dataset<Row> tagsData, AnalyticsMetrics metrics) {
        log.debug("Collecting basic tag statistics...");
        
        // Basic tag statistics - exact same as original
        long totalTags = tagsData.count();
        long uniqueTags = tagsData.select("tag").distinct().count();
        long uniqueTaggedMovies = tagsData.select("movieId").distinct().count();
        long uniqueTaggingUsers = tagsData.select("userId").distinct().count();

        metrics.addMetric("totalTags", totalTags)
               .addMetric("uniqueTags", uniqueTags)
               .addMetric("uniqueTaggedMovies", uniqueTaggedMovies)
               .addMetric("uniqueTaggingUsers", uniqueTaggingUsers)
               .addMetric("avgTagsPerMovie", (double) totalTags / uniqueTaggedMovies)
               .addMetric("avgTagsPerUser", (double) totalTags / uniqueTaggingUsers);
        
        log.debug("Basic tag statistics collected");
    }
    
    /**
     * Collects popular tags analysis - exact same as original.
     */
    private void collectPopularTags(Dataset<Row> tagsData, AnalyticsMetrics metrics) {
        log.debug("Collecting popular tags...");
        
        // Most popular tags across all movies - exact same as original
        Dataset<Row> popularTags = tagsData
                .groupBy("tag")
                .agg(
                        functions.count("tag").as("tagCount"),
                        functions.countDistinct("movieId").as("moviesTagged"),
                        functions.countDistinct("userId").as("usersWhoTagged")
                )
                .orderBy(functions.desc("tagCount"))
                .limit(20);

        // Add popular tags to metrics - exact same keys as original
        popularTags.collectAsList().forEach(row -> {
            String tag = row.getString(0);
            long tagCount = row.getLong(1);
            long moviesTagged = row.getLong(2);
            long usersWhoTagged = row.getLong(3);

            String safeTag = tag.replaceAll("[^a-zA-Z0-9]", "").toLowerCase();
            metrics.addMetric("popularTag_" + safeTag + "_name", tag)
                   .addMetric("popularTag_" + safeTag + "_count", tagCount)
                   .addMetric("popularTag_" + safeTag + "_moviesTagged", moviesTagged)
                   .addMetric("popularTag_" + safeTag + "_usersWhoTagged", usersWhoTagged);
        });
        
        log.debug("Popular tags collected");
    }
    
    /**
     * Collects tag sentiment analysis - exact same as original.
     */
    private void collectTagSentimentAnalysis(Dataset<Row> tagsData, AnalyticsMetrics metrics) {
        log.debug("Collecting tag sentiment analysis...");
        
        // Tag sentiment analysis (based on common positive/negative words) - exact same as original
        String[] positiveWords = {"good", "great", "excellent", "amazing", "awesome", "fantastic", "wonderful", "brilliant", "outstanding", "perfect"};
        String[] negativeWords = {"bad", "terrible", "awful", "horrible", "worst", "boring", "stupid", "disappointing", "waste", "sucks"};

        long positiveTags = 0;
        long negativeTags = 0;

        for (String word : positiveWords) {
            long count = tagsData.filter(functions.lower(functions.col("tag")).contains(word)).count();
            positiveTags += count;
        }

        for (String word : negativeWords) {
            long count = tagsData.filter(functions.lower(functions.col("tag")).contains(word)).count();
            negativeTags += count;
        }

        metrics.addMetric("positiveTags", positiveTags)
               .addMetric("negativeTags", negativeTags)
               .addMetric("sentimentRatio", negativeTags > 0 ? (double) positiveTags / negativeTags : positiveTags);
        
        log.debug("Tag sentiment analysis collected");
    }
    
    /**
     * Collects genre-based tag analysis - exact same as original.
     */
    private void collectGenreTagAnalysis(Dataset<Row> tagsData, Dataset<Row> moviesData, AnalyticsMetrics metrics) {
        log.debug("Collecting genre-based tag analysis...");
        
        // Genre-based tag analysis (if movies data is available) - exact same as original
        if (moviesData != null && !moviesData.isEmpty()) {
            log.debug("Analyzing tags by genre...");

            // Join tags with movies to get genre information - exact same as original
            Dataset<Row> tagsWithGenres = tagsData
                    .join(moviesData, "movieId")
                    .select(
                            functions.col("tag"),
                            functions.explode(functions.split(functions.col("genres"), "\\|")).as("genre")
                    )
                    .filter(functions.col("genre").notEqual("(no genres listed)"));

            // Most common tags per genre - exact same as original
            Dataset<Row> genreTagAnalysis = tagsWithGenres
                    .groupBy("genre", "tag")
                    .count()
                    .withColumnRenamed("count", "tagCount")
                    .withColumn("rank", functions.row_number().over(
                            org.apache.spark.sql.expressions.Window
                                    .partitionBy("genre")
                                    .orderBy(functions.desc("tagCount"))
                    ))
                    .filter(functions.col("rank").leq(3)); // Top 3 tags per genre

            // Add genre-tag insights to metrics - exact same keys as original
            genreTagAnalysis.collectAsList().forEach(row -> {
                String genre = row.getString(0);
                String tag = row.getString(1);
                long tagCount = row.getLong(2);
                int rank = row.getInt(3);

                String safeGenre = genre.replaceAll("[^a-zA-Z0-9]", "").toLowerCase();
                String safeTag = tag.replaceAll("[^a-zA-Z0-9]", "").toLowerCase();

                metrics.addMetric("genre_" + safeGenre + "_topTag" + rank + "_name", tag)
                       .addMetric("genre_" + safeGenre + "_topTag" + rank + "_count", tagCount);
            });

            metrics.addMetric("genreTagAnalysisAvailable", true);
        } else {
            metrics.addMetric("genreTagAnalysisAvailable", false);
        }
        
        log.debug("Genre-based tag analysis collected");
    }
    
    /**
     * Collects tag correlation with ratings - exact same as original.
     */
    private void collectTagRatingCorrelation(Dataset<Row> tagsData, Dataset<Row> ratingsDf, AnalyticsMetrics metrics) {
        log.debug("Collecting tag-rating correlation...");
        
        // Tag correlation with ratings (if we can join the data) - exact same as original
        Dataset<Row> tagsAlias = tagsData.alias("tags");
        Dataset<Row> ratingsAlias = ratingsDf.alias("ratings");
        
        Dataset<Row> tagsWithRatings = tagsAlias
                .join(ratingsAlias, 
                        functions.col("tags.movieId").equalTo(functions.col("ratings.movieId"))
                        .and(functions.col("tags.userId").equalTo(functions.col("ratings.userId"))))
                .select(
                        functions.col("tags.tag"),
                        functions.col("ratings.ratingActual")
                );

        if (!tagsWithRatings.isEmpty()) {
            // Average rating for movies with specific tags - exact same as original
            Dataset<Row> tagRatingCorrelation = tagsWithRatings
                    .groupBy("tag")
                    .agg(
                            functions.avg("ratingActual").as("avgRating"),
                            functions.count("ratingActual").as("ratingCount")
                    )
                    .filter(functions.col("ratingCount").gt(5)) // Only tags with sufficient data
                    .orderBy(functions.desc("avgRating"))
                    .limit(10);

            // Add tag-rating correlation to metrics - exact same keys as original
            tagRatingCorrelation.collectAsList().forEach(row -> {
                String tag = row.getString(0);
                double avgRating = row.getDouble(1);
                long ratingCount = row.getLong(2);

                String safeTag = tag.replaceAll("[^a-zA-Z0-9]", "").toLowerCase();
                metrics.addMetric("highRatedTag_" + safeTag + "_name", tag)
                       .addMetric("highRatedTag_" + safeTag + "_avgRating", avgRating)
                       .addMetric("highRatedTag_" + safeTag + "_ratingCount", ratingCount);
            });

            metrics.addMetric("tagRatingCorrelationAvailable", true);
        } else {
            metrics.addMetric("tagRatingCorrelationAvailable", false);
        }
        
        log.debug("Tag-rating correlation collected");
    }
    
    @Override
    public String getAnalyticsType() {
        return "TAG_ANALYTICS";
    }
    
    @Override
    public boolean canProcess(Dataset<Row> ratingsDf, Dataset<Row> moviesData, Dataset<Row> tagsData) {
        return ratingsDf != null && !ratingsDf.isEmpty(); // Can process even without tags data
    }
    
    @Override
    public int getPriority() {
        return 50; // Lower priority since it depends on tags data availability
    }
}
