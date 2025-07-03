package org.hiast.realtime.config;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import redis.clients.jedis.UnifiedJedis;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Tests for the AppConfig class.
 */
public class AppConfigTest {

    private AppConfig appConfig;

    @BeforeEach
    void setUp() {
        // Initialize with the test configuration file
        appConfig = new AppConfig("test-config.properties");
    }

    @Test
    @DisplayName("Should load string properties correctly")
    void shouldLoadStringPropertiesCorrectly() {
        // Test loading string properties
        assertEquals("localhost:9092", appConfig.getProperty("kafka.bootstrap.servers"));
        assertEquals("test-group", appConfig.getProperty("kafka.group.id"));
        assertEquals("test-input-topic", appConfig.getProperty("kafka.topic.input"));
        assertEquals("test-output-topic", appConfig.getProperty("kafka.topic.output"));
        assertEquals("localhost", appConfig.getProperty("redis.host"));
    }

    @Test
    @DisplayName("Should load integer properties correctly")
    void shouldLoadIntegerPropertiesCorrectly() {
        // Test loading integer properties
        assertEquals(6379, appConfig.getIntProperty("redis.port"));
        assertEquals(10, appConfig.getIntProperty("redis.pool.maxTotal"));
    }

    @Test
    @DisplayName("Should throw exception for missing properties")
    void shouldThrowExceptionForMissingProperties() {
        // Test that an exception is thrown for missing properties
        assertThrows(RuntimeException.class, () -> appConfig.getProperty("non.existent.property"));
    }

    @Test
    @DisplayName("Should throw exception for invalid integer properties")
    void shouldThrowExceptionForInvalidIntegerProperties() {
        // Test that an exception is thrown for invalid integer properties
        assertThrows(NumberFormatException.class, () -> appConfig.getIntProperty("kafka.bootstrap.servers"));
    }

    @Test
    @DisplayName("Should create UnifiedJedis instance correctly")
    void shouldCreateUnifiedJedisInstanceCorrectly() {
        // Test creating a UnifiedJedis instance
        UnifiedJedis jedis = appConfig.getUnifiedJedis();
        assertNotNull(jedis);
        // Close the Jedis instance to avoid resource leaks
        jedis.close();
    }

    @Test
    @DisplayName("Should throw exception for invalid configuration file")
    void shouldThrowExceptionForInvalidConfigurationFile() {
        // Test that an exception is thrown for an invalid configuration file
        assertThrows(RuntimeException.class, () -> new AppConfig("non-existent-config.properties"));
    }
}