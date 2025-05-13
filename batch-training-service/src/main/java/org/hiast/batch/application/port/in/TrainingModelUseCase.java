package org.hiast.batch.application.port.in;

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
     */
    void executeTrainingPipeline();
}