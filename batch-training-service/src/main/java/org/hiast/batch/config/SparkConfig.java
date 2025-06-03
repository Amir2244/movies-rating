package org.hiast.batch.config;

import org.apache.spark.SparkConf;
import org.hiast.batch.domain.exception.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import scala.Tuple2;

import java.util.Properties;

/**
 * Configuration class for Spark settings.
 * This class provides optimized Spark configurations based on the environment.
 */
public class SparkConfig {
    private static final Logger log = LoggerFactory.getLogger(SparkConfig.class);

    private final String appName;
    private final String masterUrl;
    private final Properties properties;

    /**
     * Creates a new SparkConfig with the specified application name and master URL.
     *
     * @param appName The Spark application name
     * @param masterUrl The Spark master URL
     * @param properties Additional properties for configuration
     */
    public SparkConfig(String appName, String masterUrl, Properties properties) {
        this.appName = appName;
        this.masterUrl = masterUrl;
        this.properties = properties;
    }

    /**
     * Creates a SparkConf with optimized settings based on the environment.
     *
     * @return A SparkConf with optimized settings
     */
    public SparkConf createSparkConf() {
        SparkConf conf = new SparkConf()
                .setAppName(appName)
                .setMaster(masterUrl);

        // Apply any additional properties from configuration file first
        if (properties != null) {
            for (String key : properties.stringPropertyNames()) {
                if (key.startsWith("spark.")) {
                    conf.set(key, properties.getProperty(key));
                    log.info("Applied custom Spark property from config: {}={}", key, properties.getProperty(key));
                }
            }
        }

        // Apply environment-specific settings
        if (masterUrl.startsWith("local")) {
            log.info("Applying local mode settings");
            applyLocalSettings(conf);
        } else if (masterUrl.startsWith("spark://")) {
            log.info("Applying standalone cluster mode settings");
            applyStandaloneClusterSettings(conf);
        } else if (masterUrl.equals("yarn") || masterUrl.startsWith("yarn-")) {
            log.info("Applying YARN cluster mode settings");
            applyYarnClusterSettings(conf);
        } else {
            log.warn("Unknown Spark master URL format: {}. Applying default settings.", masterUrl);
            applyDefaultSettings(conf);
        }

        // Apply common settings
        applyCommonSettings(conf);

        // Log all Spark configuration
        log.info("Final Spark configuration:");
        for (Tuple2<String, String> entry : conf.getAll()) {
            log.info("  {} = {}", entry._1(), entry._2());
        }

        return conf;
    }

    /**
     * Applies settings optimized for local mode.
     *
     * @param conf The SparkConf to modify
     */
    private void applyLocalSettings(SparkConf conf) {
        // Memory settings
        if (!conf.contains("spark.driver.memory")) {
            conf.set("spark.driver.memory", "4g");
        }

        // Local mode specific settings
        if (!conf.contains("spark.local.dir")) {
            conf.set("spark.local.dir", System.getProperty("java.io.tmpdir"));
        }

        // Performance tuning for local mode
        if (!conf.contains("spark.sql.adaptive.enabled")) {
            conf.set("spark.sql.adaptive.enabled", "true");
        }
        if (!conf.contains("spark.sql.adaptive.coalescePartitions.enabled")) {
            conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true");
        }
        if (!conf.contains("spark.sql.adaptive.skewJoin.enabled")) {
            conf.set("spark.sql.adaptive.skewJoin.enabled", "true");
        }
    }

    /**
     * Applies settings optimized for standalone cluster mode.
     *
     * @param conf The SparkConf to modify
     */
    private void applyStandaloneClusterSettings(SparkConf conf) {
        // Deployment mode
        if (!conf.contains("spark.submit.deployMode")) {
            conf.set("spark.submit.deployMode", "client");
        }
    }

    /**
     * Applies settings optimized for YARN cluster mode.
     *
     * @param conf The SparkConf to modify
     */
    private void applyYarnClusterSettings(SparkConf conf) {
        // Memory settings
        if (!conf.contains("spark.driver.memory")) {
            conf.set("spark.driver.memory", "4g");
        }
        if (!conf.contains("spark.executor.memory")) {
            conf.set("spark.executor.memory", "4g");
        }
        if (!conf.contains("spark.executor.cores")) {
            conf.set("spark.executor.cores", "2");
        }

        // YARN specific settings
        if (!conf.contains("spark.yarn.am.memory")) {
            conf.set("spark.yarn.am.memory", "2g");
        }
        if (!conf.contains("spark.yarn.am.cores")) {
            conf.set("spark.yarn.am.cores", "2");
        }
    }

    /**
     * Applies default settings when the environment cannot be determined.
     *
     * @param conf The SparkConf to modify
     */
    private void applyDefaultSettings(SparkConf conf) {
        // Conservative memory settings
        if (!conf.contains("spark.driver.memory")) {
            conf.set("spark.driver.memory", "2g");
        }
        if (!conf.contains("spark.executor.memory")) {
            conf.set("spark.executor.memory", "2g");
        }
    }

    /**
     * Applies common settings that are beneficial in all environments.
     *
     * @param conf The SparkConf to modify
     */
    private void applyCommonSettings(SparkConf conf) {


        // Off-heap memory configuration (keep here if not in properties file)
        if (!conf.contains("spark.memory.offHeap.enabled")) {
            conf.set("spark.memory.offHeap.enabled", "true");
        }
        if (!conf.contains("spark.memory.offHeap.size")) {
            conf.set("spark.memory.offHeap.size", "2g"); // Allocate 2GB for off-heap memory
        }

        // Serialization (Kryo is already set in batch_config.properties, keep as fallback)
        if (!conf.contains("spark.serializer")) {
            conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        }
        if (!conf.contains("spark.kryo.registrationRequired")) {
            conf.set("spark.kryo.registrationRequired", "false");
        }
        if (!conf.contains("spark.kryo.registrator")) {
            conf.set("spark.kryo.registrator", "org.hiast.batch.util.CustomKryoRegistrator");
        }
        
        // Additional Kryo settings for better serialization
        if (!conf.contains("spark.kryo.unsafe")) {
            conf.set("spark.kryo.unsafe", "false");
        }
        if (!conf.contains("spark.kryoserializer.buffer.max")) {
            conf.set("spark.kryoserializer.buffer.max", "1024m");
        }
        if (!conf.contains("spark.kryoserializer.buffer")) {
            conf.set("spark.kryoserializer.buffer", "64k");
        }
        
        // Settings to handle stack overflow issues
        if (!conf.contains("spark.driver.extraJavaOptions")) {
            String existingOpts = conf.get("spark.driver.extraJavaOptions", "");
            conf.set("spark.driver.extraJavaOptions", existingOpts + " -Xss4m");
        }
        if (!conf.contains("spark.executor.extraJavaOptions")) {
            String existingOpts = conf.get("spark.executor.extraJavaOptions", "");
            conf.set("spark.executor.extraJavaOptions", existingOpts + " -Xss4m");
        }


        // RDD compression (already in batch_config.properties, keep as fallback)
        if (!conf.contains("spark.rdd.compress")) {
            conf.set("spark.rdd.compress", "true");
        }
    }
}
