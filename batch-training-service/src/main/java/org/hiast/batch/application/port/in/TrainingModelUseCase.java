package org.hiast.batch.application.port.in;

import org.apache.spark.ml.recommendation.ALSModel;

/**
 * Input Port for the use case of training the recommendation model.
 * Defines the contract for initiating the model training process.
 */
public interface TrainingModelUseCase {
    /**
     * Executes the entire model training pipeline:
     * 1. Loads raw data.
     * 2. Preprocesses data.
     * 3. Trains the ALS model.
     * 4. Evaluates the model.
     * 5. Saves the computed factors.
     * 6. Saves the model to HDFS.
     */
    void executeTrainingPipeline();

    /**
     * Saves the trained ALS model to HDFS.
     * This method can be used to save the model for later use in predictions.
     * 
     * @param model The trained ALS model to save
     * @param path The HDFS path where the model should be saved
     */
    void saveModelToHDFS(ALSModel model, String path);
}
