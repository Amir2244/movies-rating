
package org.hiast.realtime.config;

import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;

import java.io.InputStream;
import java.util.Properties;

/**
 * Configuration loader for the application.
 * Reads properties from a config file.
 */
public class AppConfig {
    private final Properties properties = new Properties();

    public AppConfig(String resourceName) {
        try (InputStream stream = AppConfig.class.getClassLoader().getResourceAsStream(resourceName)) {
            if (stream == null) {
                throw new RuntimeException("Cannot find resource file: " + resourceName);
            }
            properties.load(stream);
        } catch (Exception e) {
            throw new RuntimeException("Failed to load configuration", e);
        }
    }

    public String getProperty(String key) {
        return properties.getProperty(key);
    }

    public int getIntProperty(String key) {
        return Integer.parseInt(properties.getProperty(key));
    }

    public JedisPool getJedisPool() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(getIntProperty("redis.pool.maxTotal"));
        return new JedisPool(poolConfig, getProperty("redis.host"), getIntProperty("redis.port"));
    }
}