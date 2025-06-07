package org.hiast.batch.application.pipeline.filters;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel;
import org.hiast.batch.application.pipeline.Filter;
import org.hiast.batch.application.pipeline.ALSTrainingPipelineContext;
import org.hiast.batch.config.ALSConfig;
import org.hiast.batch.domain.exception.DataLoadingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filter that splits the preprocessed data into training and test sets.
 */
public class DataSplittingFilter implements Filter<ALSTrainingPipelineContext, ALSTrainingPipelineContext> {
    private static final Logger log = LoggerFactory.getLogger(DataSplittingFilter.class);

    private final ALSConfig alsConfig;

    public DataSplittingFilter(ALSConfig alsConfig) {
        this.alsConfig = alsConfig;
    }

    @Override
    public ALSTrainingPipelineContext process(ALSTrainingPipelineContext context) {
        log.info("Splitting 'ratingsDf' (DataFrame) into training and test DataFrames...");

        Dataset<Row> ratingsDf = context.getRatingsDf();

        // Split data into training and test sets
        Dataset<Row>[] splits = ratingsDf.randomSplit(new double[]{
                alsConfig.getTrainingSplitRatio(),
                1.0 - alsConfig.getTrainingSplitRatio()
        }, alsConfig.getSeed());

        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

        // Persist the split DataFrames
        trainingData.persist(StorageLevel.MEMORY_AND_DISK());
        testData.persist(StorageLevel.MEMORY_AND_DISK());

        long trainingDataCount = trainingData.count();
        long testDataCount = testData.count();
        log.info("Training DataFrame count: {}, Test DataFrame count: {}", trainingDataCount, testDataCount);

        if (trainingData.isEmpty() || trainingDataCount == 0) {
            log.error("Training DataFrame is empty after split. Aborting.");
            throw new DataLoadingException("Training DataFrame is empty after split. Check the splitting ratio and data size.");
        }

        log.info("Schema of training DataFrame before ALS fit:");
        trainingData.printSchema();
        log.info("Sample of training DataFrame before ALS fit (showing up to 5 rows, truncate=false):");
        trainingData.show(5, false);

        if (trainingData.columns().length == 0) {
            log.error("Training DataFrame has no columns. Aborting ALS training.");
            throw new RuntimeException("Training DataFrame has no columns");
        }

        log.info("Available columns in training DataFrame for ALS: {}", String.join(", ", trainingData.columns()));

        if (!trainingData.isEmpty()) {
            try {
                log.info("First row of training DataFrame: {}", trainingData.first().toString());
            } catch (java.util.NoSuchElementException e) {
                log.warn("Could not get first row of training DataFrame as it might be empty.");
            }
        }

        // Set the training and test data in the context
        context.setTrainingData(trainingData);
        context.setTestData(testData);

        log.info("Data splitting completed successfully");
        context.markDataSplittingCompleted();
        return context;
    }
}