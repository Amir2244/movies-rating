package org.hiast.batch.adapter.driver.spark;

import org.apache.spark.sql.SparkSession;
import org.hiast.batch.application.port.in.AnalyticsUseCase;
import org.hiast.batch.application.port.out.AnalyticsPersistencePort;
import org.hiast.batch.application.port.out.RatingDataProviderPort;
import org.hiast.batch.application.service.AnalyticsService;
import org.hiast.batch.adapter.out.persistence.hdfs.HdfsRatingDataProviderAdapter;
import org.hiast.batch.adapter.out.persistence.mongodb.MongoAnalyticsPersistenceAdapter;
import org.hiast.batch.config.*;
import org.hiast.batch.domain.exception.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Dedicated Analytics Job that runs independently from model training.
 * This job focuses solely on data analytics collection and persistence.
 * Follows the same architectural patterns as BatchTrainingJob.
 */
public final class AnalyticsJob {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsJob.class);

    public static void main(String[] args) {
        log.info("Starting Dedicated Analytics Job...");

        // --- 1. Load Configuration ---
        AppConfig appConfig = new AppConfig();
        HDFSConfig hdfsConfig = appConfig.getHDFSConfig();
        MongoConfig mongoConfig = appConfig.getMongoConfig();

        log.info("Analytics Job Configuration Loaded: {}", appConfig);

        // --- 2. Initialize Spark Session with Analytics-Optimized Configuration ---
        log.info("Creating analytics-optimized Spark configuration");
        SparkConfig sparkConfig = new SparkConfig(
                "Analytics-Job-" + appConfig.getSparkAppName(),
                appConfig.getSparkMasterUrl(),
                appConfig.getProperties()
        );
        org.apache.spark.SparkConf sparkConf = sparkConfig.createSparkConf();

        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        log.info("Analytics SparkSession initialized. Spark version: {}", spark.version());
        log.info("Default FileSystem: {}", spark.sparkContext().hadoopConfiguration().get("fs.defaultFS"));

        // --- 3. Initialize Adapters (Following Hexagonal Architecture) ---
        RatingDataProviderPort ratingDataProvider = new HdfsRatingDataProviderAdapter(
                hdfsConfig.getRatingsPath()
        );

        AnalyticsPersistencePort analyticsPersistence = new MongoAnalyticsPersistenceAdapter(mongoConfig);
        log.info("Analytics adapters initialized.");

        // --- 4. Instantiate Application Service (Use Case Implementation) ---
        AnalyticsUseCase analyticsUseCase = new AnalyticsService(
                spark,
                ratingDataProvider,
                analyticsPersistence,
                hdfsConfig
        );
        log.info("Application service (AnalyticsService) instantiated.");

        // --- 5. Execute Use Case ---
        try {
            log.info("Executing AnalyticsUseCase...");
            analyticsUseCase.executeAnalyticsPipeline();
            log.info("AnalyticsUseCase executed successfully.");
        } catch (ConfigurationException e) {
            log.error("Configuration error during analytics pipeline: {}", e.getMessage(), e);
            System.exit(1);
        } catch (DataLoadingException e) {
            log.error("Data loading error during analytics pipeline: {}", e.getMessage(), e);
            System.exit(2);
        } catch (AnalyticsCollectionException e) {
            log.error("Analytics collection error during analytics pipeline: {}", e.getMessage(), e);
            System.exit(3);
        } catch (Exception e) {
            log.error("Unexpected error during analytics pipeline: {}", e.getMessage(), e);
            System.exit(4);
        } finally {
            try {
                // --- 6. Stop Spark Session ---
                log.info("Stopping Analytics SparkSession...");
                spark.stop();
                log.info("Analytics SparkSession stopped successfully.");
            } catch (Exception e) {
                log.error("Error stopping Analytics SparkSession: {}", e.getMessage(), e);
            }
        }

        log.info("Analytics Job completed successfully.");
    }

}
