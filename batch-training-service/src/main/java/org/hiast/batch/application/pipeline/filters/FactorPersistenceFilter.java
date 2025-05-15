package org.hiast.batch.application.pipeline.filters;

import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hiast.batch.application.pipeline.Filter;
import org.hiast.batch.application.pipeline.TrainingPipelineContext;
import org.hiast.batch.application.port.out.FactorPersistencePort;
import org.hiast.batch.domain.model.ModelFactors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filter that persists the model factors.
 */
public class FactorPersistenceFilter implements Filter<TrainingPipelineContext, TrainingPipelineContext> {
    private static final Logger log = LoggerFactory.getLogger(FactorPersistenceFilter.class);
    
    private final FactorPersistencePort factorPersistence;
    
    public FactorPersistenceFilter(FactorPersistencePort factorPersistence) {
        this.factorPersistence = factorPersistence;
    }
    
    @Override
    public TrainingPipelineContext process(TrainingPipelineContext context) {
        log.info("Persisting model factors...");
        
        ALSModel model = context.getModel();
        
        if (model == null) {
            log.error("Model is null. Cannot persist factors.");
            throw new RuntimeException("Model is null. Cannot persist factors.");
        }
        
        Dataset<Row> userFactors = model.userFactors().withColumnRenamed("id", "userId");
        Dataset<Row> itemFactors = model.itemFactors().withColumnRenamed("id", "itemId");
        
        factorPersistence.saveModelFactors(new ModelFactors(userFactors, itemFactors));
        log.info("Model factors persisted successfully.");
        
        // Set the factors persisted flag in the context
        context.setFactorsPersisted(true);
        
        return context;
    }
}