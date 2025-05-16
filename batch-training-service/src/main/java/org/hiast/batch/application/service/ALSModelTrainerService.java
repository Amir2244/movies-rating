package org.hiast.batch.application.service;

import org.apache.spark.sql.SparkSession;
import org.hiast.batch.application.pipeline.Pipeline;
import org.hiast.batch.application.pipeline.ALSTrainingPipelineContext;
import org.hiast.batch.application.pipeline.filters.*;
import org.hiast.batch.application.port.in.TrainingModelUseCase;
import org.hiast.batch.application.port.out.FactorCachingPort;
import org.hiast.batch.application.port.out.RatingDataProviderPort;
import org.hiast.batch.config.ALSConfig;
import org.hiast.batch.config.HDFSConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Application service implementing the TrainModelUseCase.
 * Orchestrates the model training process using injected ports for data access and persistence.
 */
public class ALSModelTrainerService implements TrainingModelUseCase {

    private static final Logger log = LoggerFactory.getLogger(ALSModelTrainerService.class);

    private final SparkSession spark;
    private final RatingDataProviderPort ratingDataProvider;
    private final FactorCachingPort factorPersistence;
    private final ALSConfig alsConfig;
    private final HDFSConfig hdfsConfig;

    public ALSModelTrainerService(SparkSession spark,
                                  RatingDataProviderPort ratingDataProvider,
                                  FactorCachingPort factorPersistence,
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
        log.info("Starting ALS model training pipeline using pipes and filter pattern...");

        try {
            // Create the pipeline context
            ALSTrainingPipelineContext context = new ALSTrainingPipelineContext(spark);

            // Create the pipeline with all filters
            Pipeline<ALSTrainingPipelineContext, ALSTrainingPipelineContext> pipeline = new Pipeline<>(ALSTrainingPipelineContext.class);

            // Add filters to the pipeline
            pipeline.addFilter(new DataLoadingFilter(ratingDataProvider))
                   .addFilter(new DataPreprocessingFilter(ratingDataProvider))
                   .addFilter(new DataSplittingFilter(alsConfig))
                   .addFilter(new ModelTrainingFilter(alsConfig))
                   .addFilter(new ModelEvaluationFilter())
                   .addFilter(new FactorPersistenceFilter(factorPersistence))
                   .addFilter(new ModelSavingFilter(hdfsConfig))
                    .addFilter(new ResultSavingFilter());

            // Execute the pipeline
            ALSTrainingPipelineContext result = pipeline.execute(context);

            // Check the result
            if (result.isModelSaved() && result.isFactorsPersisted()) {
                log.info("ALS model training pipeline finished successfully.");
            } else {
                log.warn("ALS model training pipeline finished with warnings. Model saved: {}, Factors persisted: {}",
                        result.isModelSaved(), result.isFactorsPersisted());
            }
        } catch (Exception e) {
            log.error("Error during model training pipeline: ", e);
            throw e;
        }
    }
}
