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
        // Memory settings
        if (!conf.contains("spark.driver.memory")) {
            conf.set("spark.driver.memory", "2g");
        }
        if (!conf.contains("spark.executor.memory")) {
            conf.set("spark.executor.memory", "2g");
        }
        if (!conf.contains("spark.executor.cores")) {
            conf.set("spark.executor.cores", "2");
        }

        // Deployment mode
        if (!conf.contains("spark.submit.deployMode")) {
            conf.set("spark.submit.deployMode", "client");
        }

        // Static allocation settings (default)
        if (!conf.contains("spark.dynamicAllocation.enabled")) {
            conf.set("spark.dynamicAllocation.enabled", "false");
        }
        if (!conf.contains("spark.executor.instances") &&
            !Boolean.parseBoolean(conf.get("spark.dynamicAllocation.enabled", "false"))) {
            conf.set("spark.executor.instances", "2");
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
        // Memory management
        if (!conf.contains("spark.memory.fraction")) {
            conf.set("spark.memory.fraction", "0.6");
        }
        if (!conf.contains("spark.memory.storageFraction")) {
            conf.set("spark.memory.storageFraction", "0.5");
        }

        // Serialization
        if (!conf.contains("spark.serializer")) {
            conf.set("spark.serializer", "org.apache.spark.serializer.KryoSerializer");
        }
        if (!conf.contains("spark.kryo.registrationRequired")) {
            conf.set("spark.kryo.registrationRequired", "false");
        }
        if (!conf.contains("spark.kryo.registrator")) {
            conf.set("spark.kryo.registrator", "org.hiast.batch.util.CustomKryoRegistrator");
        }

        // Shuffle settings
        if (!conf.contains("spark.shuffle.file.buffer")) {
            conf.set("spark.shuffle.file.buffer", "64k");
        }
        if (!conf.contains("spark.shuffle.spill.compress")) {
            conf.set("spark.shuffle.spill.compress", "true");
        }
        if (!conf.contains("spark.shuffle.compress")) {
            conf.set("spark.shuffle.compress", "true");
        }
        if (!conf.contains("spark.shuffle.io.maxRetries")) {
            conf.set("spark.shuffle.io.maxRetries", "10");
        }
        if (!conf.contains("spark.shuffle.io.retryWait")) {
            conf.set("spark.shuffle.io.retryWait", "30s");
        }

        // RDD compression
        if (!conf.contains("spark.rdd.compress")) {
            conf.set("spark.rdd.compress", "true");
        }

        // Network timeout
        if (!conf.contains("spark.network.timeout")) {
            conf.set("spark.network.timeout", "800s");
        }
        if (!conf.contains("spark.executor.heartbeatInterval")) {
            conf.set("spark.executor.heartbeatInterval", "60s");
        }

        // SQL settings
        if (!conf.contains("spark.sql.shuffle.partitions")) {
            conf.set("spark.sql.shuffle.partitions", "8");
        }
        if (!conf.contains("spark.default.parallelism")) {
            conf.set("spark.default.parallelism", "8");
        }
    }
}
