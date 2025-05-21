package org.hiast.batch.application.pipeline;

import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hiast.batch.domain.model.ProcessedRating;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;

/**
 * Context object that holds the state of the training pipeline.
 * This object is passed through the pipeline and updated by each filter.
 */
public class ALSTrainingPipelineContext {
    private static final Logger log = LoggerFactory.getLogger(ALSTrainingPipelineContext.class);

    private final SparkSession spark;
    private final Instant startTime;

    // Pipeline data
    private Dataset<Row> rawRatings;
    private Dataset<ProcessedRating> processedRatings;
    private Dataset<Row> ratingsDf;
    private Dataset<Row> trainingData;
    private Dataset<Row> testData;
    private ALSModel model;
    private double evaluationMetric;

    // Pipeline state
    private boolean factorsPersisted;
    private boolean modelSaved;
    private boolean resultsSaved;

    // Progress tracking
    private Instant dataLoadingCompleted;
    private Instant preprocessingCompleted;
    private Instant dataSplittingCompleted;
    private Instant modelTrainingCompleted;
    private Instant modelEvaluationCompleted;
    private Instant factorPersistenceCompleted;
    private Instant modelSavingCompleted;
    private Instant resultSavingCompleted;

    public ALSTrainingPipelineContext(SparkSession spark) {
        this.spark = spark;
        this.startTime = Instant.now();
        log.info("Pipeline started at: {}", startTime);
    }

    /**
     * Logs the progress of the pipeline.
     *
     * @param stage The current stage of the pipeline
     */
    public void logProgress(String stage) {
        Instant now = Instant.now();
        Duration elapsed = Duration.between(startTime, now);
        log.info("Pipeline progress - Stage: {} - Time elapsed: {} seconds",
                stage, elapsed.getSeconds());
    }

    /**
     * Marks the data loading stage as completed.
     */
    public void markDataLoadingCompleted() {
        this.dataLoadingCompleted = Instant.now();
        logProgress("Data Loading");
        logStageTime("Data Loading", startTime, dataLoadingCompleted);
    }

    /**
     * Marks the preprocessing stage as completed.
     */
    public void markPreprocessingCompleted() {
        this.preprocessingCompleted = Instant.now();
        logProgress("Preprocessing");
        logStageTime("Preprocessing", dataLoadingCompleted, preprocessingCompleted);
    }

    /**
     * Marks the data splitting stage as completed.
     */
    public void markDataSplittingCompleted() {
        this.dataSplittingCompleted = Instant.now();
        logProgress("Data Splitting");
        logStageTime("Data Splitting", preprocessingCompleted, dataSplittingCompleted);
    }

    /**
     * Marks the model training stage as completed.
     */
    public void markModelTrainingCompleted() {
        this.modelTrainingCompleted = Instant.now();
        logProgress("Model Training");
        logStageTime("Model Training", dataSplittingCompleted, modelTrainingCompleted);
    }

    /**
     * Marks the model evaluation stage as completed.
     */
    public void markModelEvaluationCompleted() {
        this.modelEvaluationCompleted = Instant.now();
        logProgress("Model Evaluation");
        logStageTime("Model Evaluation", modelTrainingCompleted, modelEvaluationCompleted);
    }

    /**
     * Marks the factor persistence stage as completed.
     */
    public void markFactorPersistenceCompleted() {
        this.factorPersistenceCompleted = Instant.now();
        logProgress("Factor Persistence");
        logStageTime("Factor Persistence", modelEvaluationCompleted, factorPersistenceCompleted);
    }

    /**
     * Marks the model saving stage as completed.
     */
    public void markModelSavingCompleted() {
        this.modelSavingCompleted = Instant.now();
        logProgress("Model Saving");
        logStageTime("Model Saving", factorPersistenceCompleted, modelSavingCompleted);
    }

    /**
     * Marks the result saving stage as completed.
     */
    public void markResultSavingCompleted() {
        this.resultSavingCompleted = Instant.now();
        logProgress("Result Saving");
        logStageTime("Result Saving", modelSavingCompleted, resultSavingCompleted);

        // Log total time
        Duration totalTime = Duration.between(startTime, resultSavingCompleted);
        log.info("Pipeline completed - Total time: {} seconds", totalTime.getSeconds());
    }

    /**
     * Logs the time taken for a specific stage.
     *
     * @param stageName The name of the stage
     * @param start The start time of the stage
     * @param end The end time of the stage
     */
    private void logStageTime(String stageName, Instant start, Instant end) {
        if (start != null && end != null) {
            Duration duration = Duration.between(start, end);
            log.info("Stage time - {}: {} seconds", stageName, duration.getSeconds());
        }
    }



    public SparkSession getSpark() {
        return spark;
    }

    public Dataset<Row> getRawRatings() {
        return rawRatings;
    }

    public void setRawRatings(Dataset<Row> rawRatings) {
        this.rawRatings = rawRatings;
    }

    public Dataset<ProcessedRating> getProcessedRatings() {
        return processedRatings;
    }

    public void setProcessedRatings(Dataset<ProcessedRating> processedRatings) {
        this.processedRatings = processedRatings;
    }

    public Dataset<Row> getRatingsDf() {
        return ratingsDf;
    }

    public void setRatingsDf(Dataset<Row> ratingsDf) {
        this.ratingsDf = ratingsDf;
    }

    public Dataset<Row> getTrainingData() {
        return trainingData;
    }

    public void setTrainingData(Dataset<Row> trainingData) {
        this.trainingData = trainingData;
    }

    public Dataset<Row> getTestData() {
        return testData;
    }

    public void setTestData(Dataset<Row> testData) {
        this.testData = testData;
    }

    public ALSModel getModel() {
        return model;
    }

    public void setModel(ALSModel model) {
        this.model = model;
    }

    public double getEvaluationMetric() {
        return evaluationMetric;
    }

    public void setEvaluationMetric(double evaluationMetric) {
        this.evaluationMetric = evaluationMetric;
    }

    public boolean isFactorsPersisted() {
        return factorsPersisted;
    }

    public void setFactorsPersisted(boolean factorsPersisted) {
        this.factorsPersisted = factorsPersisted;
    }

    public boolean isModelSaved() {
        return modelSaved;
    }

    public void setModelSaved(boolean modelSaved) {
        this.modelSaved = modelSaved;
    }

    public boolean isResultsSaved() {
        return resultsSaved;
    }

    public void setResultsSaved(boolean resultsSaved) {
        this.resultsSaved = resultsSaved;
    }
}