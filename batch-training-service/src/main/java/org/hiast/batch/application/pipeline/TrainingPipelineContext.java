package org.hiast.batch.application.pipeline;

import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hiast.batch.domain.model.ProcessedRating;

/**
 * Context object that holds the state of the training pipeline.
 * This object is passed through the pipeline and updated by each filter.
 */
public class TrainingPipelineContext {
    private final SparkSession spark;
    
    private Dataset<Row> rawRatings;
    private Dataset<ProcessedRating> processedRatings;
    private Dataset<Row> ratingsDf;
    private Dataset<Row> trainingData;
    private Dataset<Row> testData;
    private ALSModel model;
    private double evaluationMetric;
    private boolean factorsPersisted;
    private boolean modelSaved;
    
    public TrainingPipelineContext(SparkSession spark) {
        this.spark = spark;
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
}