package org.hiast.batch.application.pipeline.filters;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.hiast.batch.application.pipeline.Filter;
import org.hiast.batch.application.pipeline.BasePipelineContext;
import org.hiast.batch.application.pipeline.ALSTrainingPipelineContext;
import org.hiast.batch.application.port.out.RatingDataProviderPort;
import org.hiast.batch.domain.exception.DataLoadingException;
import org.hiast.batch.domain.model.ProcessedRating;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filter that preprocesses the raw ratings data.
 * Works with both ALSTrainingPipelineContext and AnalyticsPipelineContext via BasePipelineContext.
 */
public class DataPreprocessingFilter<T extends BasePipelineContext> implements Filter<T, T> {
    private static final Logger log = LoggerFactory.getLogger(DataPreprocessingFilter.class);

    private final RatingDataProviderPort ratingDataProvider;

    public DataPreprocessingFilter(RatingDataProviderPort ratingDataProvider) {
        this.ratingDataProvider = ratingDataProvider;
    }

    @Override
    public T process(T context) {
        log.info("Preprocessing ratings data into Dataset<ProcessedRating>...");

        Dataset<Row> rawRatings = context.getRawRatings();
        SparkSession spark = context.getSpark();

        // Preprocess the raw ratings
        Dataset<ProcessedRating> processedRatings = ratingDataProvider.preprocessRatings(spark, rawRatings);
        context.setProcessedRatings(processedRatings);

        log.info("Schema of 'processedRatings' (Dataset<ProcessedRating>) immediately after preprocessing (expected to be 'root' for Dataset<Bean>):");
        processedRatings.printSchema();
        log.info("Sample of 'processedRatings' (Dataset<ProcessedRating>) (showing up to 5 rows, truncate=false):");
        processedRatings.show(5, false);
        long processedRatingsCount = processedRatings.count();
        log.info("Count of 'processedRatings' (Dataset<ProcessedRating>): {}", processedRatingsCount);

        if (processedRatings.isEmpty() || processedRatingsCount == 0) {
            log.error("Dataset<ProcessedRating> 'processedRatings' is empty after preprocessing. Aborting training.");
            throw new DataLoadingException("Processed ratings dataset is empty after preprocessing. Check the data source and preprocessing logic.");
        }

        log.info("Explicitly converting 'processedRatings' to DataFrame 'ratingsDf'...");

        // Convert Dataset<ProcessedRating> to JavaRDD<ProcessedRating>
        JavaRDD<ProcessedRating> ratingsRdd = processedRatings.javaRDD();

        // Map JavaRDD<ProcessedRating> to JavaRDD<Row>
        JavaRDD<Row> rowRdd = ratingsRdd.map(processedRating -> RowFactory.create(
                processedRating.getUserId(),
                processedRating.getMovieId(),
                processedRating.getRatingActual(),
                processedRating.getTimestampEpochSeconds()
        ));

        // Define the schema explicitly
        StructType ratingsSchema = new StructType(new StructField[]{
                DataTypes.createStructField("userId", DataTypes.IntegerType, false),
                DataTypes.createStructField("movieId", DataTypes.IntegerType, false),
                DataTypes.createStructField("ratingActual", DataTypes.DoubleType, false),
                DataTypes.createStructField("timestampEpochSeconds", DataTypes.LongType, false)
        });

        // Create the DataFrame
        Dataset<Row> ratingsDf = spark.createDataFrame(rowRdd, ratingsSchema);
        context.setRatingsDf(ratingsDf);

        log.info("Schema of 'ratingsDf' (DataFrame) after explicit creation:");
        ratingsDf.printSchema();
        log.info("Sample of 'ratingsDf' (DataFrame) after explicit creation (showing up to 5 rows, truncate=false):");
        ratingsDf.show(5, false);
        long ratingsDfCount = ratingsDf.count();
        log.info("Count of 'ratingsDf' (DataFrame): {}", ratingsDfCount);

        if (ratingsDf.isEmpty() || ratingsDfCount == 0 || ratingsDf.columns().length == 0) {
            log.error("'ratingsDf' is empty or has no columns after explicit creation. Aborting.");
            throw new DataLoadingException("Ratings DataFrame is empty or has no columns after conversion. Check the data conversion logic.");
        }
        log.info("Available columns in 'ratingsDf': {}", String.join(", ", ratingsDf.columns()));

        // Persist the DataFrame if it's large and re-used
        ratingsDf.persist(StorageLevel.MEMORY_AND_DISK());
        log.info("Persisted 'ratingsDf'. Count after persist: {}", ratingsDf.count());

        context.setDataPreprocessed(true);
        log.info("Data preprocessing completed successfully");

        // Call training-specific method if it's a training context
        if (context instanceof ALSTrainingPipelineContext) {
            ((ALSTrainingPipelineContext) context).markPreprocessingCompleted();
        }

        return context;
    }
}