package org.hiast.batch.config;

import org.junit.Test;

import static org.junit.Assert.*;

public class AppConfigTest {

    @Test
    public void testLoadConfiguration() {
        // Act
        AppConfig config = new AppConfig();

        // Assert
        assertEquals("TestMovieRecsysBatchTrainer", config.getSparkAppName());
        assertEquals("local[2]", config.getSparkMasterUrl());
    }

    @Test
    public void testGetHDFSConfig() {
        // Arrange
        AppConfig config = new AppConfig();

        // Act
        HDFSConfig hdfsConfig = config.getHDFSConfig();

        // Assert
        assertNotNull(hdfsConfig);
        assertEquals("hdfs://test-namenode:8020/test/ratings.csv", hdfsConfig.getRatingsPath());
    }

    @Test
    public void testGetRedisConfig() {
        // Arrange
        AppConfig config = new AppConfig();

        // Act
        RedisConfig redisConfig = config.getRedisConfig();

        // Assert
        assertNotNull(redisConfig);
        assertEquals("test-redis", redisConfig.getHost());
        assertEquals(6380, redisConfig.getPort());
    }

    @Test
    public void testGetALSConfig() {
        // Arrange
        AppConfig config = new AppConfig();

        // Act
        ALSConfig alsConfig = config.getALSConfig();

        // Assert
        assertNotNull(alsConfig);
        assertEquals(10, alsConfig.getRank());
        assertEquals(5, alsConfig.getMaxIter());
        assertEquals(0.2, alsConfig.getRegParam(), 0.0001);
        assertEquals(42L, alsConfig.getSeed());
        assertTrue(alsConfig.isImplicitPrefs());
        assertEquals(0.5, alsConfig.getAlpha(), 0.0001);
        assertEquals(0.7, alsConfig.getTrainingSplitRatio(), 0.0001);
    }

    @Test
    public void testToString() {
        // Arrange
        AppConfig config = new AppConfig();

        // Act
        String result = config.toString();

        // Assert
        assertTrue(result.contains("sparkAppName='TestMovieRecsysBatchTrainer'"));
        assertTrue(result.contains("sparkMasterUrl='local[2]'"));
        assertTrue(result.contains("redisConfig=RedisConfig{host='test-redis', port=6380}"));
        assertTrue(result.contains("alsConfig=ALSConfig{rank=10, maxIter=5, regParam=0.2, seed=42, implicitPrefs=true, alpha=0.5}"));
    }
}