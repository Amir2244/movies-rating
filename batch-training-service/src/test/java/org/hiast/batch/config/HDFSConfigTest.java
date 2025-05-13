package org.hiast.batch.config;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HDFSConfigTest {

    @Test
    public void testConstructorAndGetter() {
        // Arrange
        String ratingsPath = "/path/to/ratings";

        // Act
        HDFSConfig config = new HDFSConfig(ratingsPath);

        // Assert
        assertEquals(ratingsPath, config.getRatingsPath());
    }

    @Test
    public void testToString() {
        // Arrange
        String ratingsPath = "/path/to/ratings";
        HDFSConfig config = new HDFSConfig(ratingsPath);

        // Act
        String result = config.toString();

        // Assert
        assertTrue(result.contains("ratingsPath='/path/to/ratings'"));
    }
}