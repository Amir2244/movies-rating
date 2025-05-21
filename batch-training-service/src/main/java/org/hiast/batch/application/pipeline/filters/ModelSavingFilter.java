package org.hiast.batch.application.pipeline.filters;

import org.apache.spark.ml.recommendation.ALSModel;
import org.hiast.batch.application.pipeline.Filter;
import org.hiast.batch.application.pipeline.ALSTrainingPipelineContext;
import org.hiast.batch.config.HDFSConfig;
import org.hiast.batch.domain.exception.ModelPersistenceException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Filter that saves the trained model to HDFS.
 */
public class ModelSavingFilter implements Filter<ALSTrainingPipelineContext, ALSTrainingPipelineContext> {
    private static final Logger log = LoggerFactory.getLogger(ModelSavingFilter.class);

    private final HDFSConfig hdfsConfig;

    public ModelSavingFilter(HDFSConfig hdfsConfig) {
        this.hdfsConfig = hdfsConfig;
    }

    @Override
    public ALSTrainingPipelineContext process(ALSTrainingPipelineContext context) {
        log.info("Saving ALS model to HDFS...");

        ALSModel model = context.getModel();
        String path = hdfsConfig.getModelSavePath();

        if (model == null) {
            log.error("Model is null. Cannot save to HDFS.");
            throw new ModelPersistenceException("Model is null. Cannot save to HDFS. Check if model training completed successfully.");
        }

        try {
            log.info("Saving ALS model to HDFS at path: {}", path);
            model.write().overwrite().save(path);
            log.info("ALS model saved successfully to HDFS.");

            // Set the model-saved flag in the context
            context.setModelSaved(true);
            log.info("Model saved flag set in context with evaluation: {}", context.getEvaluationMetric());
            context.markModelSavingCompleted();
        } catch (IOException e) {
            log.error("Failed to save ALS model to HDFS at path: {}", path, e);
            throw new ModelPersistenceException("Failed to save ALS model to HDFS at path: " + path, e);
        }

        return context;
    }
}