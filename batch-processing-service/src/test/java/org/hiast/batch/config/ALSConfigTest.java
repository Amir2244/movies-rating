package org.hiast.batch.config;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertFalse;

public class ALSConfigTest {

    @Test
    public void testConstructorAndGetters() {
        // Arrange
        int rank = 10;
        int maxIter = 20;
        double regParam = 0.1;
        long seed = 42L;
        boolean implicitPrefs = true;
        double alpha = 0.01;
        double trainingSplitRatio = 0.8;

        // Act
        ALSConfig config = new ALSConfig(rank, maxIter, regParam, seed, implicitPrefs, alpha, trainingSplitRatio);

        // Assert
        assertEquals(rank, config.getRank());
        assertEquals(maxIter, config.getMaxIter());
        assertEquals(regParam, config.getRegParam(), 0.0001);
        assertEquals(seed, config.getSeed());
        assertTrue(config.isImplicitPrefs());
        assertEquals(alpha, config.getAlpha(), 0.0001);
        assertEquals(trainingSplitRatio, config.getTrainingSplitRatio(), 0.0001);
    }

    @Test
    public void testToString() {
        // Arrange
        ALSConfig config = new ALSConfig(10, 20, 0.1, 42L, true, 0.01, 0.8);

        // Act
        String result = config.toString();

        // Assert
        assertTrue(result.contains("rank=10"));
        assertTrue(result.contains("maxIter=20"));
        assertTrue(result.contains("regParam=0.1"));
        assertTrue(result.contains("seed=42"));
        assertTrue(result.contains("implicitPrefs=true"));
        assertTrue(result.contains("alpha=0.01"));
    }

    @Test
    public void testWithImplicitPrefsFalse() {
        // Arrange & Act
        ALSConfig config = new ALSConfig(10, 20, 0.1, 42L, false, 0.01, 0.8);

        // Assert
        assertFalse(config.isImplicitPrefs());
    }
}