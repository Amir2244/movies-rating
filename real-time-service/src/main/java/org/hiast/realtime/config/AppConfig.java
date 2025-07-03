
package org.hiast.realtime.config;

import redis.clients.jedis.*;

import java.io.InputStream;
import java.util.Properties;

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
        String value = properties.getProperty(key);
        if (value == null) {
            throw new RuntimeException("Property not found: " + key);
        }
        return value;
    }

    public int getIntProperty(String key) {
        return Integer.parseInt(properties.getProperty(key));
    }

    /**
     * Creates a UnifiedJedis client. For a standalone Redis instance,
     * JedisPooled is a simple, thread-safe implementation.
     * @return A UnifiedJedis instance.
     */
    public UnifiedJedis getUnifiedJedis() {
        HostAndPort hostAndPort = new HostAndPort(getProperty("redis.host"), getIntProperty("redis.port"));
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(getIntProperty("redis.pool.maxTotal"));
        return new JedisPooled(hostAndPort);
    }
}
