package org.hiast.batch.application.pipeline.filters;

import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hiast.batch.application.pipeline.Filter;
import org.hiast.batch.application.pipeline.TrainingPipelineContext;
import org.hiast.batch.config.ALSConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filter that trains the ALS model using the training data.
 */
public class ModelTrainingFilter implements Filter<TrainingPipelineContext, TrainingPipelineContext> {
    private static final Logger log = LoggerFactory.getLogger(ModelTrainingFilter.class);
    
    private final ALSConfig alsConfig;
    
    public ModelTrainingFilter(ALSConfig alsConfig) {
        this.alsConfig = alsConfig;
    }
    
    @Override
    public TrainingPipelineContext process(TrainingPipelineContext context) {
        log.info("Training ALS model with params: Rank={}, MaxIter={}, RegParam={}, Seed={}",
                alsConfig.getRank(), alsConfig.getMaxIter(), alsConfig.getRegParam(),
                alsConfig.getSeed());
        
        Dataset<Row> trainingData = context.getTrainingData();
        
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
        
        ALSModel model = als.fit(trainingData);
        log.info("ALS model training completed.");
        
        // Set the model in the context
        context.setModel(model);
        
        return context;
    }
}