package org.hiast.batch.config;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loads and provides application-wide configurations.
 * For a real application, consider using a more robust configuration library
 * like TypeSafe Config or Spring Configuration.
 */
public class AppConfig {
    private static final Logger log = LoggerFactory.getLogger(AppConfig.class);
    private static final String CONFIG_FILE = "batch_config.properties";
    private final Properties properties;

    public AppConfig() {
        properties = new Properties();
        try (InputStream input = getClass().getClassLoader().getResourceAsStream(CONFIG_FILE)) {
            if (input == null) {
                log.error("Sorry, unable to find " + CONFIG_FILE);
                throw new RuntimeException("Configuration file " + CONFIG_FILE + " not found in classpath.");
            }
            properties.load(input);
            log.info("Application configuration loaded successfully from {}", CONFIG_FILE);
        } catch (IOException ex) {
            log.error("Error loading configuration file " + CONFIG_FILE, ex);
            throw new RuntimeException("Error loading configuration file " + CONFIG_FILE, ex);
        }
    }

    public String getSparkAppName() {
        return properties.getProperty("spark.app.name", "BatchRecsysTrainingDefault");
    }

    public String getSparkMasterUrl() {
        return properties.getProperty("spark.master.url", "spark://spark-master:7077");
    }

    public HDFSConfig getHDFSConfig() {
        return new HDFSConfig(
                properties.getProperty("data.input.ratings.hdfs.path", "hdfs:///user/your_user/movielens/ratings.csv"), // Default HDFS path for ratings
                properties.getProperty("data.output.model.hdfs.path", "hdfs:///user/your_user/models/als_model") // Default HDFS path for model
        );
    }

    public RedisConfig getRedisConfig() {
        return new RedisConfig(
                properties.getProperty("redis.host", "localhost"),
                Integer.parseInt(properties.getProperty("redis.port", "6379"))
        );
    }

    public ALSConfig getALSConfig() {
        return new ALSConfig(
                Integer.parseInt(properties.getProperty("als.rank", "10")),
                Integer.parseInt(properties.getProperty("als.maxIter", "10")),
                Double.parseDouble(properties.getProperty("als.regParam", "0.1")),
                Long.parseLong(properties.getProperty("als.seed", "12345L")),
                Boolean.parseBoolean(properties.getProperty("als.implicitPrefs", "false")),
                Double.parseDouble(properties.getProperty("als.alpha", "1.0")),
                Double.parseDouble(properties.getProperty("als.trainingSplitRatio", "1.0"))
        );
    }

    public MongoConfig getMongoConfig() {
        return new MongoConfig(
                properties.getProperty("mongodb.host", "mongodb"),
                Integer.parseInt(properties.getProperty("mongodb.port", "27017")),
                properties.getProperty("mongodb.database", "movie-recommendations"),
                properties.getProperty("mongodb.collection.recommendations", "user-recommendations"),
                properties.getProperty("mongodb.collection.analytics", "data-analytics")
        );
    }

    @Override
    public String toString() {
        return "AppConfig{" +
                "sparkAppName='" + getSparkAppName() + '\'' +
                ", sparkMasterUrl='" + getSparkMasterUrl() + '\'' +
                ", hdfsConfig=" + getHDFSConfig() +
                ", redisConfig=" + getRedisConfig() +
                ", alsConfig=" + getALSConfig() +
                ", mongoConfig=" + getMongoConfig() +
                '}';
    }
}
