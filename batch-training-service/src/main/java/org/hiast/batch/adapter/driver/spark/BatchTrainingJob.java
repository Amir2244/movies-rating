package org.hiast.batch.adapter.driver.spark;


import org.apache.spark.SparkConf;
import org.apache.spark.memory.SparkOutOfMemoryError;
import org.apache.spark.sql.SparkSession;
import org.hiast.batch.adapter.out.persistence.hdfs.HdfsRatingDataProviderAdapter;
import org.hiast.batch.adapter.out.persistence.redis.RedisFactorPersistenceAdapter;
import org.hiast.batch.application.port.in.TrainingModelUseCase;
import org.hiast.batch.application.port.out.FactorPersistencePort;
import org.hiast.batch.application.port.out.RatingDataProviderPort;
import org.hiast.batch.application.service.ALSModelTrainerService;
import org.hiast.batch.config.ALSConfig;
import org.hiast.batch.config.AppConfig;
import org.hiast.batch.config.HDFSConfig;
import org.hiast.batch.config.RedisConfig;
import org.hiast.batch.domain.model.ProcessedRating;
import org.hiast.ids.MovieId;
import org.hiast.ids.UserId;
import org.hiast.model.RatingValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.Collections;

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
        ALSConfig alsConfig = appConfig.getALSConfig();

        log.info("Application Configuration Loaded: {}", appConfig);

        // --- 2. Initialize Spark Session ---
        SparkConf sparkConf = new SparkConf()
                .setAppName(appConfig.getSparkAppName())
                .set("spark.executor.memory", "6g")
                .set("spark.driver.memory", "6g")
                .set("spark.memory.fraction", "0.8")
                .set("spark.memory.storageFraction", "0.4")
                .set("spark.shuffle.memoryFraction", "0.8")
                .set("spark.shuffle.spill.compress", "true")
                .set("spark.default.parallelism", "12")
                .set("spark.sql.shuffle.partitions", "12")
                .registerKryoClasses(Arrays.asList(ProcessedRating.class, UserId.class, MovieId.class,
                        RatingValue.class).toArray(new Class[4]))
                .setMaster(appConfig.getSparkMasterUrl());

        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        log.info("SparkSession initialized. Spark version: {}", spark.version());
        log.info("Default FileSystem: {}", spark.sparkContext().hadoopConfiguration().get("fs.defaultFS"));

        // --- 3. Instantiate Adapters (Infrastructure Layer Implementations) ---
        RatingDataProviderPort ratingDataProvider = new HdfsRatingDataProviderAdapter(hdfsConfig.getRatingsPath());
        FactorPersistencePort factorPersistence = new RedisFactorPersistenceAdapter(redisConfig.getHost(), redisConfig.getPort());
        log.info("Infrastructure adapters instantiated.");

        // --- 4. Instantiate Application Service (Use Case Implementation) ---
        TrainingModelUseCase trainModelUseCase = new ALSModelTrainerService(
                spark,
                ratingDataProvider,
                factorPersistence,
                alsConfig,
                hdfsConfig
        );
        log.info("Application service (ALSModelTrainerService) instantiated.");

        // --- 5. Execute Use Case ---
        try {
            log.info("Executing TrainModelUseCase...");
            trainModelUseCase.executeTrainingPipeline();
            log.info("TrainModelUseCase executed successfully.");
        } catch (SparkOutOfMemoryError e) {
            log.error("Spark out of memory error during model training pipeline. Consider increasing memory settings or reducing data size: ", e);
            spark.catalog().clearCache();
        } catch (OutOfMemoryError e) {
            log.error("JVM out of memory error during model training pipeline. Increase driver/executor memory: ", e);
            System.gc();
        } catch (Exception e) {
            log.error("Error during model training pipeline: ", e);
        } finally {
            try {
                // --- 6. Stop Spark Session ---
                log.info("Stopping SparkSession...");
                spark.stop();
                log.info("SparkSession stopped. Batch Training Job finished.");
            } catch (Exception e) {
                log.error("Error while stopping SparkSession: ", e);
            }
        }
    }
}
