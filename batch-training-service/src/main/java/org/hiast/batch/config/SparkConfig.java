package org.hiast.batch.config;

import org.apache.spark.SparkConf;
import org.hiast.batch.domain.exception.ConfigurationException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        // Determine environment type based on master URL
        if (masterUrl.startsWith("local")) {
            applyLocalSettings(conf);
        } else if (masterUrl.startsWith("spark://")) {
            applyStandaloneClusterSettings(conf);
        } else if (masterUrl.equals("yarn") || masterUrl.startsWith("yarn-")) {
            applyYarnClusterSettings(conf);
        } else {
            log.warn("Unknown Spark master URL format: {}. Applying default settings.", masterUrl);
            applyDefaultSettings(conf);
        }

        // Apply any additional properties
        if (properties != null) {
            for (String key : properties.stringPropertyNames()) {
                if (key.startsWith("spark.")) {
                    conf.set(key, properties.getProperty(key));
                    log.debug("Applied custom Spark property: {}={}", key, properties.getProperty(key));
                }
            }
        }

        return conf;
    }

    /**
     * Applies settings optimized for local mode.
     *
     * @param conf The SparkConf to modify
     */
    private void applyLocalSettings(SparkConf conf) {
        log.info("Applying local mode Spark settings");

        // Memory settings for local mode
        conf.set("spark.driver.memory", "4g");
        conf.set("spark.executor.memory", "4g");

        // Serialization settings
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");

        // Local mode specific settings
        conf.set("spark.local.dir", System.getProperty("java.io.tmpdir"));

        // Apply common settings
        applyCommonSettings(conf);
    }

    /**
     * Applies settings optimized for standalone cluster mode.
     *
     * @param conf The SparkConf to modify
     */
    private void applyStandaloneClusterSettings(SparkConf conf) {
        log.info("Applying standalone cluster mode Spark settings");

        // Memory settings for standalone cluster
        conf.set("spark.driver.memory", "6g");
        conf.set("spark.executor.memory", "6g");
        conf.set("spark.executor.cores", "4");

        // Dynamic allocation settings - only enable if shuffle service is available
        if (Boolean.parseBoolean(properties.getProperty("spark.shuffle.service.enabled", "false"))) {
            conf.set("spark.dynamicAllocation.enabled", "true");
            conf.set("spark.dynamicAllocation.initialExecutors", "2");
            conf.set("spark.dynamicAllocation.minExecutors", "1");
            conf.set("spark.dynamicAllocation.maxExecutors", "10");
        } else {
            // Use static allocation if shuffle service is not available
            conf.set("spark.dynamicAllocation.enabled", "false");
            conf.set("spark.executor.instances", "2");
        }

        // Apply common settings
        applyCommonSettings(conf);
    }

    /**
     * Applies settings optimized for YARN cluster mode.
     *
     * @param conf The SparkConf to modify
     */
    private void applyYarnClusterSettings(SparkConf conf) {
        log.info("Applying YARN cluster mode Spark settings");

        // Memory settings for YARN cluster
        conf.set("spark.driver.memory", "8g");
        conf.set("spark.executor.memory", "8g");
        conf.set("spark.executor.cores", "4");
        conf.set("spark.executor.instances", "4");

        // YARN specific settings
        conf.set("spark.yarn.am.memory", "2g");
        conf.set("spark.yarn.am.cores", "2");

        // Dynamic allocation settings - only enable if shuffle service is available
        if (Boolean.parseBoolean(properties.getProperty("spark.shuffle.service.enabled", "false"))) {
            conf.set("spark.shuffle.service.enabled", "true");
            conf.set("spark.dynamicAllocation.enabled", "true");
            conf.set("spark.dynamicAllocation.initialExecutors", "2");
            conf.set("spark.dynamicAllocation.minExecutors", "1");
            conf.set("spark.dynamicAllocation.maxExecutors", "20");
        } else {
            // Use static allocation if shuffle service is not available
            conf.set("spark.dynamicAllocation.enabled", "false");
            conf.set("spark.executor.instances", "4");
        }

        // Apply common settings
        applyCommonSettings(conf);
    }

    /**
     * Applies default settings when the environment cannot be determined.
     *
     * @param conf The SparkConf to modify
     */
    private void applyDefaultSettings(SparkConf conf) {
        log.info("Applying default Spark settings");

        // Conservative memory settings
        conf.set("spark.driver.memory", "4g");
        conf.set("spark.executor.memory", "4g");

        // Apply common settings
        applyCommonSettings(conf);
    }

    /**
     * Applies common settings that are beneficial in all environments.
     *
     * @param conf The SparkConf to modify
     */
    private void applyCommonSettings(SparkConf conf) {
        // Memory management
        conf.set("spark.memory.fraction", "0.8");
        conf.set("spark.memory.storageFraction", "0.3");

        // Serialization
        conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        conf.set("spark.kryo.registrationRequired", "false");
        conf.set("spark.kryo.registrator", "org.hiast.batch.util.CustomKryoRegistrator");

        // Shuffle settings
        conf.set("spark.shuffle.file.buffer", "64k");
        conf.set("spark.shuffle.spill.compress", "true");
        conf.set("spark.shuffle.compress", "true");
        conf.set("spark.shuffle.io.maxRetries", "10");
        conf.set("spark.shuffle.io.retryWait", "30s");

        // RDD compression
        conf.set("spark.rdd.compress", "true");

        // Network timeout
        conf.set("spark.network.timeout", "800s");
        conf.set("spark.executor.heartbeatInterval", "60s");

        // SQL settings
        conf.set("spark.sql.shuffle.partitions", "200");
        conf.set("spark.sql.autoBroadcastJoinThreshold", "10485760"); // 10 MB

        // Speculative execution
        conf.set("spark.speculation", "true");
    }
}
