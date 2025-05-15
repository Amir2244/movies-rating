package org.hiast.batch.application.pipeline.filters;

import org.apache.spark.ml.recommendation.ALSModel;
import org.hiast.batch.application.pipeline.Filter;
import org.hiast.batch.application.pipeline.TrainingPipelineContext;
import org.hiast.batch.config.HDFSConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Filter that saves the trained model to HDFS.
 */
public class ModelSavingFilter implements Filter<TrainingPipelineContext, TrainingPipelineContext> {
    private static final Logger log = LoggerFactory.getLogger(ModelSavingFilter.class);

    private final HDFSConfig hdfsConfig;

    public ModelSavingFilter(HDFSConfig hdfsConfig) {
        this.hdfsConfig = hdfsConfig;
    }

    @Override
    public TrainingPipelineContext process(TrainingPipelineContext context) {
        log.info("Saving ALS model to HDFS...");

        ALSModel model = context.getModel();
        String path = hdfsConfig.getModelSavePath();

        if (model == null) {
            log.error("Model is null. Cannot save to HDFS.");
            throw new RuntimeException("Model is null. Cannot save to HDFS.");
        }

        try {
            log.info("Saving ALS model to HDFS at path: {}", path);
            model.write().overwrite().save(path);
            log.info("ALS model saved successfully to HDFS.");

            // Set the model-saved flag in the context
            context.setModelSaved(true);
            log.info("Model saved flag set in context with evaluation: {}", context.getEvaluationMetric());
        } catch (IOException e) {
            log.error("Failed to save ALS model to HDFS at path: {}", path, e);
            throw new RuntimeException("Failed to save ALS model to HDFS", e);
        }

        return context;
    }
}