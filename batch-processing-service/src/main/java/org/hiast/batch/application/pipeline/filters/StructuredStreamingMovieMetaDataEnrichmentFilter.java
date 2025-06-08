//package org.hiast.batch.application.pipeline.filters;
//
//import org.apache.spark.ml.recommendation.ALSModel;
//import org.apache.spark.sql.*;
//import org.apache.spark.sql.streaming.StreamingQuery;
//import org.apache.spark.sql.streaming.Trigger;
//import org.apache.spark.sql.types.DataTypes;
//import org.apache.spark.sql.types.StructType;
//import org.apache.spark.api.java.function.FlatMapFunction;
//import org.hiast.batch.application.pipeline.ALSTrainingPipelineContext;
//import org.hiast.batch.application.pipeline.Filter;
//import org.hiast.batch.application.port.out.ResultPersistencePort;
//import org.hiast.batch.application.port.out.RatingDataProviderPort;
//import org.hiast.batch.domain.exception.ModelPersistenceException;
//import org.slf4j.Logger;
//import org.slf4j.LoggerFactory;
//
//import java.io.Serializable;
//import java.time.Instant;
//import java.util.*;
//import java.util.concurrent.TimeUnit;
//
//import static org.apache.spark.sql.functions.*;
//import static org.hiast.batch.application.pipeline.filters.StreamingMovieMetaDataEnrichmentFilter.getRowDataset;
//
///**
// * Structured Streaming version of the movie metadata enrichment filter.
// *
// * This implementation uses Spark's Structured Streaming API to:
// * 1. Create a streaming DataFrame from user IDs
// * 2. Process micro-batches through the ALS model
// * 3. Enrich with movie metadata
// * 4. Stream results to persistence layer
// *
// * Benefits:
// * - Automatic memory management
// * - Built-in fault tolerance
// * - Progress tracking
// * - Backpressure handling
// */
//public class StructuredStreamingMovieMetaDataEnrichmentFilter
//    implements Filter<ALSTrainingPipelineContext, ALSTrainingPipelineContext>, Serializable {
//
//    private static final long serialVersionUID = 1L;
//    private static final Logger log = LoggerFactory.getLogger(StructuredStreamingMovieMetaDataEnrichmentFilter.class);
//    private static final int NUM_RECOMMENDATIONS_PER_USER = 10;
//    private static final int MICRO_BATCH_SIZE = 5000;
//    private static final boolean ENABLE_ENRICHMENT = true;
//
//    private final ResultPersistencePort resultPersistence;
//    private final RatingDataProviderPort ratingDataProvider;
//
//    public StructuredStreamingMovieMetaDataEnrichmentFilter(ResultPersistencePort resultPersistence,
//                                                           RatingDataProviderPort ratingDataProvider) {
//        this.resultPersistence = resultPersistence;
//        this.ratingDataProvider = ratingDataProvider;
//    }
//
//    @Override
//    public ALSTrainingPipelineContext process(ALSTrainingPipelineContext context) {
//        log.info("Starting structured streaming movie metadata enrichment...");
//
//        ALSModel model = context.getModel();
//        if (model == null) {
//            throw new ModelPersistenceException("Model is null. Cannot generate recommendations.");
//        }
//
//        try {
//            SparkSession spark = SparkSession.active();
//
//            // Get all unique user IDs
//            Dataset<Row> allUsers = context.getRatingsDf()
//                .select("userId")
//                .distinct()
//                .coalesce(1); // Single partition for rate control
//
//            long totalUsers = allUsers.count();
//            log.info("Total users to process: {}", totalUsers);
//
//            // Load movie metadata once (if enrichment is enabled)
//            Dataset<Row> movieMetadata = null;
//            if (ENABLE_ENRICHMENT) {
//                movieMetadata = loadMovieMetadata(spark);
//                if (movieMetadata != null) {
//                    // Register as temp view for use in streaming operations
//                    movieMetadata.createOrReplaceTempView("movie_metadata");
//                }
//            }
//
//            // Process using structured streaming
//            boolean success = processWithStructuredStreaming(
//                allUsers, model, movieMetadata, spark, totalUsers);
//
//            // Clean up
//            if (movieMetadata != null) {
//                movieMetadata.unpersist();
//                spark.catalog().dropTempView("movie_metadata");
//            }
//
//            context.setResultsSaved(success);
//            context.markResultSavingCompleted();
//            return context;
//
//        } catch (Exception e) {
//            log.error("Error processing recommendations: {}", e.getMessage(), e);
//            throw new ModelPersistenceException("Error processing recommendations", e);
//        }
//    }
//
//    /**
//     * Process users using Structured Streaming
//     */
//    private boolean processWithStructuredStreaming(Dataset<Row> allUsers,
//                                                 ALSModel model,
//                                                 Dataset<Row> movieMetadata,
//                                                 SparkSession spark,
//                                                 long totalUsers) {
//        final Instant now = Instant.now();
//        final String modelVersion = "ALS-" + UUID.randomUUID().toString().substring(0, 8);
//
//        try {
//            // Create a rate source that will emit user IDs in controlled batches
//            Dataset<Row> streamingUsers = spark
//                .readStream()
//                .format("rate")
//                .option("rowsPerSecond", MICRO_BATCH_SIZE)
//                .option("numPartitions", 1)
//                .load()
//                .join(
//                    allUsers.withColumn("row_id", monotonically_increasing_id()),
//                    col("value").equalTo(col("row_id"))
//                )
//                .select("userId");
//
//            // Define the streaming transformation
//            Dataset<Row> recommendations = streamingUsers
//                .flatMap(
//                    new UserRecommendationGenerator(model, NUM_RECOMMENDATIONS_PER_USER, spark),
//                    Encoders.row(createRecommendationSchema())
//                );
//
//            // Enrich with movie metadata if enabled
//            if (ENABLE_ENRICHMENT && movieMetadata != null) {
//                recommendations = enrichRecommendations(recommendations, spark);
//            }
//
//            // Define the output sink
//            StreamingQuery query = recommendations
//                .writeStream()
//                .foreachBatch((Dataset<Row> batchDF, Long batchId) -> {
//                    processMicroBatch(batchDF, batchId, now, modelVersion, totalUsers);
//                })
//                .outputMode("append")
//                .trigger(Trigger.ProcessingTime(1, TimeUnit.SECONDS))
//                .start();
//
//            // Wait for completion
//            query.awaitTermination();
//
//            log.info("Structured streaming processing completed successfully");
//            return true;
//
//        } catch (Exception e) {
//            log.error("Error in structured streaming processing: {}", e.getMessage(), e);
//            return false;
//        }
//    }
//
//    /**
//     * Generator function for recommendations
//     */
//    private static class UserRecommendationGenerator
//        implements FlatMapFunction<Row, Row>, Serializable {
//
//        private static final long serialVersionUID = 1L;
//        private final ALSModel model;
//        private final int numRecommendations;
//        private final SparkSession spark;
//
//        public UserRecommendationGenerator(ALSModel model, int numRecommendations, SparkSession spark) {
//            this.model = model;
//            this.numRecommendations = numRecommendations;
//            this.spark = spark;
//        }
//
//        @Override
//        public Iterator<Row> call(Row userRow) throws Exception {
//            List<Row> results = new ArrayList<>();
//            int userId = userRow.getInt(0);
//
//            // Create single user DataFrame
//            Dataset<Row> userDF = spark.createDataFrame(
//                Collections.singletonList(RowFactory.create(userId)),
//                new StructType().add("userId", DataTypes.IntegerType)
//            );
//
//            // Generate recommendations for this user
//            Dataset<Row> recs = model.recommendForUserSubset(userDF, numRecommendations);
//
//            // Flatten results
//            List<Row> recRows = recs.collectAsList();
//            if (!recRows.isEmpty()) {
//                Row recRow = recRows.get(0);
//                List<Row> recommendations = recRow.getList(1);
//
//                for (Row rec : recommendations) {
//                    results.add(RowFactory.create(
//                        userId,
//                        rec.getInt(0),  // movieId
//                        rec.getFloat(1) // rating
//                    ));
//                }
//            }
//
//            return results.iterator();
//        }
//    }
//
//    /**
//     * Create schema for recommendation rows
//     */
//    private StructType createRecommendationSchema() {
//        return new StructType()
//            .add("userId", DataTypes.IntegerType)
//            .add("movieId", DataTypes.IntegerType)
//            .add("rating", DataTypes.FloatType);
//    }
//
//    /**
//     * Enrich recommendations with movie metadata
//     */
//    private Dataset<Row> enrichRecommendations(Dataset<Row> recommendations, SparkSession spark) {
//        return recommendations
//            .join(
//                spark.table("movie_metadata"),
//                recommendations.col("movieId").equalTo(col("movieId")),
//                "left_outer"
//            )
//            .select(
//                recommendations.col("userId"),
//                recommendations.col("movieId"),
//                recommendations.col("rating"),
//                col("title"),
//                col("genres")
//            );
//    }
//
//    /**
//     * Process a micro-batch of recommendations
//     */
//    private void processMicroBatch(Dataset<Row> batchDF,
//                                 Long batchId,
//                                 Instant now,
//                                 String modelVersion,
//                                 long totalUsers) {
//        long batchSize = batchDF.count();
//        log.info("Processing micro-batch {} with {} recommendations", batchId, batchSize);
//
//        // Group by user
//        Dataset<Row> grouped = batchDF
//            .groupBy("userId")
//            .agg(collect_list(
//                struct(
//                    col("movieId"),
//                    col("rating"),
//                    when(col("title").isNotNull(), col("title")).otherwise(lit("")),
//                    when(col("genres").isNotNull(), col("genres")).otherwise(lit(""))
//                )
//            ).alias("recommendations"));
//
//        // Collect and save
//        List<UserRecommendations> userRecommendations = new ArrayList<>();
//
//        grouped.collectAsList().forEach(row -> {
//            int userId = row.getInt(0);
//            List<Row> recs = row.getList(1);
//
//            List<MovieRecommendation> movieRecs = new ArrayList<>();
//            for (Row rec : recs) {
//                int movieId = rec.getInt(0);
//                float rating = rec.getFloat(1);
//                String title = rec.getString(2);
//                String genresStr = rec.getString(3);
//
//                if (!title.isEmpty()) {
//                    List<String> genres = genresStr.isEmpty() ?
//                        Collections.emptyList() : Arrays.asList(genresStr.split("\\|"));
//
//                    MovieMetaData metaData = new MovieMetaData(movieId, title, genres);
//                    movieRecs.add(new MovieRecommendation(userId, movieId, rating, now, metaData));
//                } else {
//                    movieRecs.add(new MovieRecommendation(userId, movieId, rating, now));
//                }
//            }
//
//            userRecommendations.add(new UserRecommendations(userId, movieRecs, now, modelVersion));
//        });
//
//        // Save in batches
//        saveInBatches(userRecommendations);
//
//        // Log progress
//        long processedSoFar = batchId * MICRO_BATCH_SIZE;
//        if (processedSoFar > 0 && processedSoFar % 10000 == 0) {
//            log.info("Progress: ~{}/{} users processed ({:.1f}%)",
//                processedSoFar, totalUsers, (processedSoFar * 100.0) / totalUsers);
//        }
//    }
//
//    /**
//     * Save recommendations in batches
//     */
//    private void saveInBatches(List<UserRecommendations> recommendations) {
//        int batchSize = 100;
//
//        StreamingMovieMetaDataEnrichmentFilter.batchIterating(recommendations, batchSize, resultPersistence, log);
//    }
//
//    /**
//     * Load movie metadata with optimization
//     */
//    private Dataset<Row> loadMovieMetadata(SparkSession spark) {
//        return getRowDataset(spark, ratingDataProvider);
//    }
//}