package org.hiast.batch.adapter.driver.spark;


import org.apache.spark.SparkConf;
import org.apache.spark.memory.SparkOutOfMemoryError;
import org.apache.spark.sql.SparkSession;
import org.hiast.batch.adapter.out.memory.redis.RedisFactorCachingAdapter;
import org.hiast.batch.adapter.out.persistence.hdfs.HdfsRatingDataProviderAdapter;
import org.hiast.batch.adapter.out.persistence.mongodb.MongoAnalyticsPersistenceAdapter;
import org.hiast.batch.adapter.out.persistence.mongodb.MongoResultPersistenceAdapter;
import org.hiast.batch.application.port.in.TrainingModelUseCase;
import org.hiast.batch.application.port.out.AnalyticsPersistencePort;
import org.hiast.batch.application.port.out.FactorCachingPort;
import org.hiast.batch.application.port.out.RatingDataProviderPort;
import org.hiast.batch.domain.exception.BatchTrainingException;
import org.hiast.batch.domain.exception.ConfigurationException;
import org.hiast.batch.domain.exception.DataLoadingException;
import org.hiast.batch.domain.exception.ModelPersistenceException;
import org.hiast.batch.domain.exception.ModelTrainingException;
import org.hiast.batch.application.port.out.ResultPersistencePort;
import org.hiast.batch.application.service.ALSModelTrainerService;
import org.hiast.batch.config.ALSConfig;
import org.hiast.batch.config.AppConfig;
import org.hiast.batch.config.HDFSConfig;
import org.hiast.batch.config.MongoConfig;
import org.hiast.batch.config.RedisConfig;
import org.hiast.batch.config.SparkConfig;
import org.hiast.batch.domain.model.ModelFactors;
import org.hiast.batch.domain.model.ProcessedRating;
import org.hiast.ids.MovieId;
import org.hiast.ids.UserId;
import org.hiast.model.RatingValue;
import org.hiast.model.factors.ItemFactor;
import org.hiast.model.factors.UserFactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Main entry point for the Spark batch training job.
 * This class acts as the "Driving Adapter" in the Hexagonal Architecture.
 * It initializes Spark, sets up dependencies (ports and their adapters),
 * and invokes the application use case.
 */
public final class BatchTrainingJob {
    private static final Logger log = LoggerFactory.getLogger(BatchTrainingJob.class);

    public static void main(String[] args) {
        log.info("Initializing Batch Training Job...");

        // --- 1. Load Configuration ---
        AppConfig appConfig = new AppConfig();
        HDFSConfig hdfsConfig = appConfig.getHDFSConfig();
        RedisConfig redisConfig = appConfig.getRedisConfig();
        MongoConfig mongoConfig = appConfig.getMongoConfig();
        ALSConfig alsConfig = appConfig.getALSConfig();

        log.info("Application Configuration Loaded: {}", appConfig);

        // --- 2. Initialize Spark Session with optimized configuration ---
        log.info("Creating optimized Spark configuration");
        SparkConfig sparkConfig = new SparkConfig(
                appConfig.getSparkAppName(),
                appConfig.getSparkMasterUrl(),
                appConfig.getProperties()
        );
        SparkConf sparkConf = sparkConfig.createSparkConf();

        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        log.info("SparkSession initialized. Spark version: {}", spark.version());
        log.info("Default FileSystem: {}", spark.sparkContext().hadoopConfiguration().get("fs.defaultFS"));

        // --- 3. Instantiate Adapters (Infrastructure Layer Implementations) ---
        RatingDataProviderPort ratingDataProvider = new HdfsRatingDataProviderAdapter(hdfsConfig.getRatingsPath());
        FactorCachingPort factorPersistence = new RedisFactorCachingAdapter(redisConfig.getHost(), redisConfig.getPort());
        ResultPersistencePort resultPersistence = new MongoResultPersistenceAdapter(mongoConfig);
        AnalyticsPersistencePort analyticsPersistence = new MongoAnalyticsPersistenceAdapter(mongoConfig);
        log.info("Infrastructure adapters instantiated.");

        // --- 4. Instantiate Application Service (Use Case Implementation) ---
        TrainingModelUseCase trainModelUseCase = new ALSModelTrainerService(
                spark,
                ratingDataProvider,
                factorPersistence,
                resultPersistence,
                analyticsPersistence,
                alsConfig,
                hdfsConfig
        );
        log.info("Application service (ALSModelTrainerService) instantiated.");

        // --- 5. Execute Use Case ---
        try {
            log.info("Executing TrainModelUseCase...");
            trainModelUseCase.executeTrainingPipeline();
            log.info("TrainModelUseCase executed successfully.");
        } catch (ConfigurationException e) {
            log.error("Configuration error during model training pipeline: {}", e.getMessage(), e);
            System.exit(1);
        } catch (DataLoadingException e) {
            log.error("Data loading error during model training pipeline: {}", e.getMessage(), e);
            System.exit(2);
        } catch (ModelTrainingException e) {
            log.error("Model training error during model training pipeline: {}", e.getMessage(), e);
            System.exit(3);
        } catch (ModelPersistenceException e) {
            log.error("Model persistence error during model training pipeline: {}", e.getMessage(), e);
            System.exit(4);
        } catch (BatchTrainingException e) {
            log.error("Batch training error during model training pipeline: {}", e.getMessage(), e);
            System.exit(5);
        } catch (SparkOutOfMemoryError e) {
            log.error("Spark out of memory error during model training pipeline. Consider increasing memory settings or reducing data size: {}", e.getMessage(), e);
            spark.catalog().clearCache();
            System.exit(6);
        } catch (OutOfMemoryError e) {
            log.error("JVM out of memory error during model training pipeline. Increase driver/executor memory: {}", e.getMessage(), e);
            System.gc();
            System.exit(7);
        } catch (Exception e) {
            log.error("Unexpected error during model training pipeline: {}", e.getMessage(), e);
            System.exit(8);
        } finally {
            try {
                // --- 6. Stop Spark Session ---
                log.info("Stopping SparkSession...");
                spark.stop();
                log.info("SparkSession stopped. Batch Training Job finished.");
            } catch (Exception e) {
                log.error("Error while stopping SparkSession: {}", e.getMessage(), e);
            }
        }
    }
}
