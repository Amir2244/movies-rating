package org.hiast.batch.application.service;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.hiast.batch.application.port.in.TrainingModelUseCase;
import org.hiast.batch.application.port.out.FactorPersistencePort;
import org.hiast.batch.application.port.out.RatingDataProviderPort;
import org.hiast.batch.config.ALSConfig;
import org.hiast.batch.config.HDFSConfig;
import org.hiast.batch.domain.model.ModelFactors;
import org.hiast.batch.domain.model.ProcessedRating;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Arrays;

/**
 * Application service implementing the TrainModelUseCase.
 * Orchestrates the model training process using injected ports for data access and persistence.
 */
public class ALSModelTrainerService implements TrainingModelUseCase {

    private static final Logger log = LoggerFactory.getLogger(ALSModelTrainerService.class);

    private final SparkSession spark;
    private final RatingDataProviderPort ratingDataProvider;
    private final FactorPersistencePort factorPersistence;
    private final ALSConfig alsConfig;
    private final HDFSConfig hdfsConfig;

    public ALSModelTrainerService(SparkSession spark,
                                  RatingDataProviderPort ratingDataProvider,
                                  FactorPersistencePort factorPersistence,
                                  ALSConfig alsConfig,
                                  HDFSConfig hdfsConfig) {
        this.spark = spark;
        this.ratingDataProvider = ratingDataProvider;
        this.factorPersistence = factorPersistence;
        this.alsConfig = alsConfig;
        this.hdfsConfig = hdfsConfig;
    }

    @Override
    public void executeTrainingPipeline() {
        log.info("Starting ALS model training pipeline...");

        // 1. Load and Preprocess Data
        log.info("Loading raw ratings...");
        Dataset<Row> rawRatings = ratingDataProvider.loadRawRatings(spark);
        if (rawRatings.isEmpty()) {
            log.error("No raw ratings data loaded. Aborting training.");
            return;
        }

        log.info("Preprocessing ratings data into Dataset<ProcessedRating>...");
        Dataset<ProcessedRating> ratingsProcessedDs = ratingDataProvider.preprocessRatings(spark, rawRatings);

        log.info("Schema of 'ratingsProcessedDs' (Dataset<ProcessedRating>) immediately after preprocessing (expected to be 'root' for Dataset<Bean>):");
        ratingsProcessedDs.printSchema();
        log.info("Sample of 'ratingsProcessedDs' (Dataset<ProcessedRating>) (showing up to 5 rows, truncate=false):");
        ratingsProcessedDs.show(5, false);
        long ratingsProcessedDsCount = ratingsProcessedDs.count();
        log.info("Count of 'ratingsProcessedDs' (Dataset<ProcessedRating>): {}", ratingsProcessedDsCount);

        if (ratingsProcessedDs.isEmpty() || ratingsProcessedDsCount == 0) {
            log.error("Dataset<ProcessedRating> 'ratingsProcessedDs' is empty after preprocessing. Aborting training.");
            return;
        }

        log.info("Explicitly converting 'ratingsProcessedDs' to DataFrame 'ratingsDf' BEFORE split...");

        // 1. Convert Dataset<ProcessedRating> to JavaRDD<ProcessedRating>
        JavaRDD<ProcessedRating> ratingsRdd = ratingsProcessedDs.javaRDD();

        // 2. Map JavaRDD<ProcessedRating> to JavaRDD<Row>
        JavaRDD<Row> rowRdd = ratingsRdd.map(processedRating -> RowFactory.create(
                processedRating.getUserId(),
                processedRating.getMovieId(),
                processedRating.getRatingActual(),
                processedRating.getTimestampEpochSeconds()
        ));

        // 3. Define the schema explicitly
        StructType ratingsSchema = new StructType(new StructField[]{
                DataTypes.createStructField("userId", DataTypes.IntegerType, false),
                DataTypes.createStructField("movieId", DataTypes.IntegerType, false),
                DataTypes.createStructField("ratingActual", DataTypes.DoubleType, false),
                DataTypes.createStructField("timestampEpochSeconds", DataTypes.LongType, false)
        });

        // 4. Create the DataFrame
        Dataset<Row> ratingsDf = spark.createDataFrame(rowRdd, ratingsSchema);

        log.info("Schema of 'ratingsDf' (DataFrame) after explicit creation and before split:");
        ratingsDf.printSchema();
        log.info("Sample of 'ratingsDf' (DataFrame) after explicit creation and before split (showing up to 5 rows, truncate=false):");
        ratingsDf.show(5, false);
        long ratingsDfCount = ratingsDf.count();
        log.info("Count of 'ratingsDf' (DataFrame): {}", ratingsDfCount);

        if (ratingsDf.isEmpty() || ratingsDfCount == 0 || ratingsDf.columns().length == 0) {
            log.error("'ratingsDf' is empty or has no columns after explicit creation. Aborting.");
            return;
        }
        log.info("Available columns in 'ratingsDf': {}", String.join(", ", ratingsDf.columns()));


        // Persist the DataFrame if it's large and re-used
        ratingsDf.persist(StorageLevel.MEMORY_AND_DISK());
        log.info("Persisted 'ratingsDf'. Count after persist: {}", ratingsDf.count());


        // 2. Split data (now splitting a DataFrame)
        log.info("Splitting 'ratingsDf' (DataFrame) into training and test DataFrames...");
        Dataset<Row>[] splits = ratingsDf.randomSplit(new double[]{alsConfig.getTrainingSplitRatio(),
                1.0 - alsConfig.getTrainingSplitRatio()}, alsConfig.getSeed());
        Dataset<Row> trainingDataRow = splits[0];
        Dataset<Row> testDataRow = splits[1];

        // Persist the split DataFrames
        trainingDataRow.persist(StorageLevel.MEMORY_AND_DISK());
        testDataRow.persist(StorageLevel.MEMORY_AND_DISK());

        long trainingDataRowCount = trainingDataRow.count();
        long testDataRowCount = testDataRow.count();
        log.info("Training DataFrame 'trainingDataRow' count: {}, Test DataFrame 'testDataRow' count: {}", trainingDataRowCount, testDataRowCount);

        if (trainingDataRow.isEmpty() || trainingDataRowCount == 0) {
            log.error("Training DataFrame 'trainingDataRow' is empty after split. Aborting.");
            return;
        }

        log.info("Schema of 'trainingDataRow' (DataFrame for training) before ALS fit:");
        trainingDataRow.printSchema();
        log.info("Sample of 'trainingDataRow' (DataFrame for training) before ALS fit (showing up to 5 rows, truncate=false):");
        trainingDataRow.show(5, false);

        if (trainingDataRow.columns().length == 0) {
            log.error("'trainingDataRow' has no columns. Aborting ALS training.");
            return;
        }
        log.info("Available columns in 'trainingDataRow' for ALS: {}", String.join(", ", trainingDataRow.columns()));
        if (!trainingDataRow.isEmpty()) {
            try {
                log.info("First row of 'trainingDataRow': {}", trainingDataRow.first().toString());
            } catch (java.util.NoSuchElementException e) {
                log.warn("Could not get first row of 'trainingDataRow' as it might be empty.");
            }
        }

        // 3. Train ALS Model
        log.info("Training ALS model with params: Rank={}, MaxIter={}, RegParam={}, Seed={}",
                alsConfig.getRank(), alsConfig.getMaxIter(), alsConfig.getRegParam(),
                alsConfig.getSeed());

        String userCol = "userId";
        String itemCol = "movieId";
        String ratingCol = "ratingActual";

        log.info("Using column names for ALS: userCol={}, itemCol={}, ratingCol={}", userCol, itemCol, ratingCol);

        ALS als = new ALS()
                .setMaxIter(alsConfig.getMaxIter())
                .setRegParam(alsConfig.getRegParam())
                .setRank(alsConfig.getRank())
                .setUserCol(userCol)
                .setItemCol(itemCol)
                .setRatingCol(ratingCol)
                .setColdStartStrategy("drop")
                .setSeed(alsConfig.getSeed())
                .setImplicitPrefs(alsConfig.isImplicitPrefs())
                .setAlpha(alsConfig.getAlpha());

        ALSModel model = als.fit(trainingDataRow);
        log.info("ALS model training completed.");

        // 4. Evaluate Model (if test data exists)
        if (!testDataRow.isEmpty() && testDataRowCount > 0) {
            log.info("Evaluating model on test data (testDataRow)...");
            log.info("Schema of 'testDataRow':");
            testDataRow.printSchema();
            testDataRow.show(5, false);

            if (testDataRow.columns().length > 0) {
                boolean hasUserColTest = Arrays.asList(testDataRow.columns()).contains(userCol);
                boolean hasItemColTest = Arrays.asList(testDataRow.columns()).contains(itemCol);
                boolean hasRatingColTest = Arrays.asList(testDataRow.columns()).contains(ratingCol);

                if (!hasUserColTest || !hasItemColTest || !hasRatingColTest) {
                    log.error("Test DataFrame 'testDataRow' does not contain all required columns for model transformation. UserCol present: {}, ItemCol present: {}, RatingCol present: {}. Available: {}",
                            hasUserColTest, hasItemColTest, hasRatingColTest, String.join(", ", testDataRow.columns()));
                } else {
                    Dataset<Row> predictions = model.transform(testDataRow);
                    log.info("Predictions sample:");
                    predictions.show(5, false);

                    RegressionEvaluator evaluator = new RegressionEvaluator()
                            .setMetricName("rmse")
                            .setLabelCol(ratingCol)
                            .setPredictionCol("prediction");
                    double rmse = evaluator.evaluate(predictions);
                    log.info("Model Evaluation - Root Mean Squared Error (RMSE) on test data = {}", rmse);
                }
            } else {
                log.warn("Test DataFrame 'testDataRow' has no columns. Skipping evaluation.");
            }
        } else {
            log.warn("Test DataFrame 'testDataRow' is empty or count is zero, skipping model evaluation.");
        }

        // 5. Persist Factors
        log.info("Persisting model factors...");
        Dataset<Row> userFactors = model.userFactors().withColumnRenamed("id", "userId");
        Dataset<Row> itemFactors = model.itemFactors().withColumnRenamed("id", "itemId");

        factorPersistence.saveModelFactors(new ModelFactors(userFactors, itemFactors));
        log.info("Model factors persisted successfully.");

        // 6. Save model to HDFS
        saveModelToHDFS(model, hdfsConfig.getModelSavePath());

        log.info("ALS model training pipeline finished (explicit DF creation before split).");
    }

    @Override
    public void saveModelToHDFS(ALSModel model, String path) {
        try {
            log.info("Saving ALS model to HDFS at path: {}", path);
            model.write().save(path);
            log.info("ALS model saved successfully to HDFS.");
        } catch (IOException e) {
            log.error("Failed to save ALS model to HDFS at path: {}", path, e);
            throw new RuntimeException("Failed to save ALS model to HDFS", e);
        }
    }
}
