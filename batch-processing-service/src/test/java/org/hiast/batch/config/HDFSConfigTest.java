package org.hiast.batch.config;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class HDFSConfigTest {

    @Test
    public void testConstructorAndGetter() {
        // Arrange
        String ratingsPath = "/path/to/ratings";
        String savingPath = "/path/to/save";

        // Act
        HDFSConfig config = new HDFSConfig(ratingsPath, savingPath);

        // Assert
        assertEquals(ratingsPath, config.getRatingsPath());
    }

    @Test
    public void testToString() {
        // Arrange
        String ratingsPath = "/path/to/ratings";
        String savingPath = "/path/to/save";
        HDFSConfig config = new HDFSConfig(ratingsPath, savingPath);

        // Act
        String result = config.toString();

        // Assert
        assertTrue(result.contains("ratingsPath='/path/to/ratings'"));
    }
}