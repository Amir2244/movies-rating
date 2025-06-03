package org.hiast.batch.application.pipeline.filters;

import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.*;
import org.hiast.batch.application.pipeline.ALSTrainingPipelineContext;
import org.hiast.batch.application.pipeline.Filter;
import org.hiast.batch.application.port.out.ResultPersistencePort;
import org.hiast.batch.application.port.out.RatingDataProviderPort;
import org.hiast.batch.domain.exception.ModelPersistenceException;
import org.hiast.batch.domain.model.MovieMetaData;
import org.hiast.batch.domain.model.MovieRecommendation;
import org.hiast.batch.domain.model.UserRecommendations;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.time.Instant;
import java.util.*;

import static org.apache.spark.sql.functions.*;

/**
 * Streaming enrichment filter that generates and processes recommendations
 * in small batches to avoid memory issues.
 * <p>
 * Instead of generating all recommendations at once, we:
 * 1. Get all user IDs
 * 2. Process them in batches (e.g., 5000 users at a time)
 * 3. For each batch: generate recommendations, enrich, and save
 * 4. Move to the next batch
 * <p>
 * This ensures we never have more than BATCH_SIZE users' recommendations in memory.
 */
public class StreamingMovieMetaDataEnrichmentFilter implements Filter<ALSTrainingPipelineContext, ALSTrainingPipelineContext>, Serializable {
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(StreamingMovieMetaDataEnrichmentFilter.class);
    private static final int NUM_RECOMMENDATIONS_PER_USER = 10;
    private static final int USER_BATCH_SIZE = 5000; // Process 5000 users at a time
    private static final boolean ENABLE_ENRICHMENT = true;
    
    private final ResultPersistencePort resultPersistence;
    private final RatingDataProviderPort ratingDataProvider;
    
    public StreamingMovieMetaDataEnrichmentFilter(ResultPersistencePort resultPersistence, 
                                                RatingDataProviderPort ratingDataProvider) {
        this.resultPersistence = resultPersistence;
        this.ratingDataProvider = ratingDataProvider;
    }
    
    @Override
    public ALSTrainingPipelineContext process(ALSTrainingPipelineContext context) {
        log.info("Starting streaming movie metadata enrichment...");
        
        ALSModel model = context.getModel();
        if (model == null) {
            throw new ModelPersistenceException("Model is null. Cannot generate recommendations.");
        }
        
        try {
            SparkSession spark = SparkSession.active();
            
            // Get all unique user IDs
            Dataset<Row> allUsers = context.getRatingsDf()
                .select("userId")
                .distinct();
            
            long totalUsers = allUsers.count();
            log.info("Total users to process: {}", totalUsers);
            
            // Load movie metadata once (if enrichment is enabled)
            Dataset<Row> movieMetadata = null;
            if (ENABLE_ENRICHMENT) {
                movieMetadata = loadMovieMetadata(spark);
            }
            
            // Process users in batches
            boolean success = processUsersInBatches(allUsers, model, movieMetadata, spark, totalUsers);
            
            // Clean up
            if (movieMetadata != null) {
                movieMetadata.unpersist();
            }
            
            context.setResultsSaved(success);
            context.markResultSavingCompleted();
            return context;
            
        } catch (Exception e) {
            log.error("Error processing recommendations: {}", e.getMessage(), e);
            throw new ModelPersistenceException("Error processing recommendations", e);
        }
    }
    
    /**
     * Process users in batches, generating recommendations for each batch
     */
    private boolean processUsersInBatches(Dataset<Row> allUsers, 
                                        ALSModel model,
                                        Dataset<Row> movieMetadata,
                                        SparkSession spark,
                                        long totalUsers) {
        final Instant now = Instant.now();
        final String modelVersion = "ALS-" + UUID.randomUUID().toString().substring(0, 8);
        
        // Convert to list and process in chunks
        List<Row> userRows = allUsers.collectAsList();
        int processedCount = 0;
        int failedBatches = 0;
        
        for (int i = 0; i < userRows.size(); i += USER_BATCH_SIZE) {
            int endIdx = Math.min(i + USER_BATCH_SIZE, userRows.size());
            List<Row> batchRows = userRows.subList(i, endIdx);
            
            // Create dataset for this batch
            Dataset<Row> batchUsers = spark.createDataFrame(
                batchRows,
                new org.apache.spark.sql.types.StructType()
                    .add("userId", org.apache.spark.sql.types.DataTypes.IntegerType)
            );
            
            try {
                // Generate recommendations ONLY for this batch of users
                Dataset<Row> batchRecommendations = model.recommendForUserSubset(
                    batchUsers, 
                    NUM_RECOMMENDATIONS_PER_USER
                );
                
                // Process and save this batch
                processBatch(batchRecommendations, movieMetadata, now, modelVersion);
                
                processedCount += batchRows.size();
                log.info("Processed {}/{} users ({}%)",
                    processedCount, totalUsers, 
                    (processedCount * 100.0) / totalUsers);
                
            } catch (Exception e) {
                log.error("Error processing batch {}-{}: {}", i, endIdx, e.getMessage());
                failedBatches++;
            }
        }
        
        if (failedBatches > 0) {
            log.warn("Completed with {} failed batches", failedBatches);
        } else {
            log.info("Successfully processed all {} users", totalUsers);
        }
        
        return failedBatches == 0;
    }
    
    /**
     * Process a single batch of recommendations
     */
    private void processBatch(Dataset<Row> batchRecommendations,
                            Dataset<Row> movieMetadata,
                            Instant now,
                            String modelVersion) {
        
        try {
            List<UserRecommendations> recommendations;
            if (!ENABLE_ENRICHMENT || movieMetadata == null) {
                // Collect and save without enrichment
                recommendations = collectWithoutEnrichment(
                        batchRecommendations, now, modelVersion);
                
                // Save outside Spark operation
            } else {
                // Collect and save with enrichment
                recommendations = collectWithEnrichment(
                        batchRecommendations, movieMetadata, now, modelVersion);
                
                // Save outside Spark operation
            }
            saveInBatches(recommendations);
        } catch (Exception e) {
            log.error("Error in processBatch: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to process batch", e);
        }
    }
    
    /**
     * Collect recommendations without enrichment
     */
    private List<UserRecommendations> collectWithoutEnrichment(Dataset<Row> recommendations,
                                                              Instant now,
                                                              String modelVersion) {
        List<UserRecommendations> result = new ArrayList<>();
        
        List<Row> rows = recommendations.collectAsList();
        for (Row userRow : rows) {
            int userId = userRow.getInt(0);
            List<Row> recs = userRow.getList(1);
            
            List<MovieRecommendation> movieRecs = new ArrayList<>();
            for (Row rec : recs) {
                int movieId = rec.getInt(0);
                float rating = rec.getFloat(1);
                movieRecs.add(new MovieRecommendation(userId, movieId, rating, now));
            }
            
            result.add(new UserRecommendations(userId, movieRecs, now, modelVersion));
        }
        
        return result;
    }
    
    /**
     * Collect recommendations with enrichment
     */
    private List<UserRecommendations> collectWithEnrichment(Dataset<Row> recommendations,
                                                          Dataset<Row> movieMetadata,
                                                          Instant now,
                                                          String modelVersion) {
        // Flatten recommendations
        Dataset<Row> flatRecs = recommendations
            .select(
                col("userId"),
                explode(col("recommendations")).as("rec")
            )
            .select(
                col("userId"),
                col("rec.movieId").alias("movieId"),
                col("rec.rating").alias("rating")
            );
        
        // Join with movie metadata
        Dataset<Row> enriched = flatRecs.join(
            movieMetadata,
            flatRecs.col("movieId").equalTo(movieMetadata.col("movieId")),
            "left_outer"
        ).select(
            flatRecs.col("userId"),
            flatRecs.col("movieId"),
            flatRecs.col("rating"),
            movieMetadata.col("title"),
            movieMetadata.col("genres")
        );
        
        // Group by user
        Dataset<Row> grouped = enriched
            .groupBy("userId")
            .agg(collect_list(
                struct(
                    col("movieId"),
                    col("rating"),
                    col("title"),
                    col("genres")
                )
            ).alias("recommendations"));
        
        // Collect and convert
        List<UserRecommendations> result = new ArrayList<>();
        List<Row> rows = grouped.collectAsList();
        
        for (Row userRow : rows) {
            int userId = userRow.getInt(0);
            List<Row> recs = userRow.getList(1);
            
            List<MovieRecommendation> movieRecs = new ArrayList<>();
            for (Row rec : recs) {
                int movieId = rec.getInt(0);
                float rating = rec.getFloat(1);
                
                if (!rec.isNullAt(2)) {
                    String title = rec.getString(2);
                    String genresStr = rec.isNullAt(3) ? "" : rec.getString(3);
                    List<String> genres = genresStr.isEmpty() ? 
                        Collections.emptyList() : Arrays.asList(genresStr.split("\\|"));
                    
                    MovieMetaData metaData = new MovieMetaData(movieId, title, genres);
                    movieRecs.add(new MovieRecommendation(userId, movieId, rating, now, metaData));
                } else {
                    movieRecs.add(new MovieRecommendation(userId, movieId, rating, now));
                }
            }
            
            result.add(new UserRecommendations(userId, movieRecs, now, modelVersion));
        }
        
        return result;
    }
    
    /**
     * Save recommendations in batches
     */
    private void saveInBatches(List<UserRecommendations> recommendations) {
        int batchSize = 1000;

        batchIterating(recommendations, batchSize, resultPersistence, log);
    }

    static void batchIterating(List<UserRecommendations> recommendations, int batchSize, ResultPersistencePort resultPersistence, Logger log) {
        for (int i = 0; i < recommendations.size(); i += batchSize) {
            int end = Math.min(i + batchSize, recommendations.size());
            List<UserRecommendations> batch = recommendations.subList(i, end);

            try {
                resultPersistence.saveUserRecommendations(batch);
            } catch (Exception e) {
                log.error("Error saving batch of {} recommendations: {}", batch.size(), e.getMessage());
            }
        }
    }

    /**
     * Load movie metadata with optimization
     */
    private Dataset<Row> loadMovieMetadata(SparkSession spark) {
        return getRowDataset(spark, ratingDataProvider);
    }

    @Nullable
    static Dataset<Row> getRowDataset(SparkSession spark, RatingDataProviderPort ratingDataProvider) {
        StreamingMovieMetaDataEnrichmentFilter.log.info("Loading movie metadata for enrichment...");
        try {
            Dataset<Row> rawMovies = ratingDataProvider.loadRawMovies(spark);
            if (rawMovies == null || rawMovies.isEmpty()) {
                StreamingMovieMetaDataEnrichmentFilter.log.warn("No movie metadata available");
                return null;
            }

            // Only load essential columns
            Dataset<Row> movies = rawMovies
                .selectExpr(
                    "CAST(movieId AS INT) AS movieId",
                    "CAST(title AS STRING) AS title",
                    "CAST(genres AS STRING) AS genres"
                );

            long count = movies.count();
            if (count < 50000) {
                StreamingMovieMetaDataEnrichmentFilter.log.info("Broadcasting {} movies for efficient lookups", count);
                return broadcast(movies).cache();
            } else {
                StreamingMovieMetaDataEnrichmentFilter.log.info("Using regular dataset for {} movies", count);
                return movies.cache();
            }

        } catch (Exception e) {
            StreamingMovieMetaDataEnrichmentFilter.log.error("Error loading movie metadata: {}", e.getMessage(), e);
            return null;
        }
    }
} 