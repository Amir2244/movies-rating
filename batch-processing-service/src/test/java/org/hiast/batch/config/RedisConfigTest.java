package org.hiast.batch.config;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class RedisConfigTest {

    @Test
    public void testConstructorAndGetters() {
        // Arrange
        String host = "localhost";
        int port = 6379;

        // Act
        RedisConfig config = new RedisConfig(host, port);

        // Assert
        assertEquals(host, config.getHost());
        assertEquals(port, config.getPort());
    }

    @Test
    public void testToString() {
        // Arrange
        String host = "redis-server";
        int port = 6380;
        RedisConfig config = new RedisConfig(host, port);

        // Act
        String result = config.toString();

        // Assert
        assertTrue(result.contains("host='redis-server'"));
        assertTrue(result.contains("port=6380"));
    }
}