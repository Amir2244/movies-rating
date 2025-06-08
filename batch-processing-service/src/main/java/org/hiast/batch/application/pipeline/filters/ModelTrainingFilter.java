package org.hiast.batch.application.pipeline.filters;

import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.storage.StorageLevel; // Import StorageLevel
import org.hiast.batch.application.pipeline.Filter;
import org.hiast.batch.application.pipeline.ALSTrainingPipelineContext;
import org.hiast.batch.config.ALSConfig;
import org.hiast.batch.domain.exception.ModelTrainingException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filter that trains the ALS model using the training data.
 * OPTIMIZED VERSION: Includes performance-related ALS parameters.
 */
public class ModelTrainingFilter implements Filter<ALSTrainingPipelineContext, ALSTrainingPipelineContext> {
    private static final Logger log = LoggerFactory.getLogger(ModelTrainingFilter.class);

    private final ALSConfig alsConfig;

    public ModelTrainingFilter(ALSConfig alsConfig) {
        this.alsConfig = alsConfig;
    }

    @Override
    public ALSTrainingPipelineContext process(ALSTrainingPipelineContext context) {
        log.info("Training ALS model with params: Rank={}, MaxIter={}, RegParam={}, Seed={}, ImplicitPrefs={}, Alpha={}",
                alsConfig.getRank(), alsConfig.getMaxIter(), alsConfig.getRegParam(),
                alsConfig.getSeed(), alsConfig.isImplicitPrefs(), alsConfig.getAlpha());

        Dataset<Row> trainingData = context.getTrainingData(); // Assumed to be already cached by DataSplittingFilter

        String userCol = "userId";
        String itemCol = "movieId";
        String ratingCol = "ratingActual";

        log.info("Using column names for ALS: userCol={}, itemCol={}, ratingCol={}", userCol, itemCol, ratingCol);

        // --- OPTIMIZATIONS: Set performance-related ALS parameters ---
        // It's best if these come from ALSConfig, but providing defaults if not.
        int numUserBlocks = 4; // Assuming getNumUserBlocks() exists in ALSConfig
        int numItemBlocks = 4; // Assuming getNumItemBlocks() exists in ALSConfig
        int checkpointInterval =5; // Assuming getCheckpointInterval() exists

        // If not available from ALSConfig, set some common defaults (tune these!)
        // if (numUserBlocks <= 0) numUserBlocks = 100; // Example default, tune based on data size
        // if (numItemBlocks <= 0) numItemBlocks = 100; // Example default
        // if (checkpointInterval <=0) checkpointInterval = 10; // Example default

        log.info("ALS performance params: NumUserBlocks={}, NumItemBlocks={}, CheckpointInterval={}",
                numUserBlocks, numItemBlocks, checkpointInterval);


        ALS als = new ALS()
                .setMaxIter(alsConfig.getMaxIter())
                .setRegParam(alsConfig.getRegParam())
                .setRank(alsConfig.getRank())
                .setUserCol(userCol)
                .setItemCol(itemCol)
                .setRatingCol(ratingCol)
                .setColdStartStrategy("drop") // Good for preventing errors with new users/items in evaluation
                .setSeed(alsConfig.getSeed())
                .setImplicitPrefs(alsConfig.isImplicitPrefs())
                .setAlpha(alsConfig.getAlpha())
                // --- OPTIMIZATIONS ADDED/MODIFIED ---
                .setNumUserBlocks(numUserBlocks)           // Tune this!
                .setNumItemBlocks(numItemBlocks)           // Tune this!
                .setIntermediateStorageLevel("MEMORY_ONLY") ;      // More robust storage

        // Only set checkpointInterval if a checkpoint directory is configured in SparkContext
        // and the interval is positive.
        if (context.getSpark().sparkContext().getCheckpointDir().isDefined() && checkpointInterval > 0) {
            als.setCheckpointInterval(checkpointInterval); // Helps with long lineages and fault tolerance
            log.info("ALS checkpointInterval set to {} as checkpoint directory is available.", checkpointInterval);
        } else if (checkpointInterval > 0) {
            log.warn("ALS checkpointInterval was configured to {} but no Spark checkpoint directory is set. Checkpoint will not be active.", checkpointInterval);
        }


        try {
            log.info("Starting ALS model fitting...");
            ALSModel model = als.fit(trainingData);
            log.info("ALS model training completed successfully.");

            // Remove any debug/show calls on the model for production runs
            // model.userFactors().show(5, false); // Example of a debug line to remove
            // model.itemFactors().show(5, false); // Example of a debug line to remove

            context.setModel(model);
            context.markModelTrainingCompleted();
        } catch (Exception e) {
            log.error("Error during ALS model training: {}", e.getMessage(), e);
            // Consider logging more details about trainingData if possible, e.g. trainingData.count()
            // but be careful as .count() is an action and could trigger recomputation if trainingData wasn't properly cached or if fit failed early.
            log.error("Training data sample (first 3 rows, might be partial if not cached or error was early):");
            try {
                trainingData.show(3,false);
            } catch (Exception showEx) {
                log.error("Could not show trainingData sample: {}", showEx.getMessage());
            }
            throw new ModelTrainingException("Failed to train ALS model. Check Spark logs for executor errors (OOMs, etc.) and review ALS parameters.", e);
        }

        return context;
    }
}