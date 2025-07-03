package org.hiast.analyticsapi.config;

import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the MongoConfig class.
 */
class MongoConfigTest {

    @Test
    void testDefaultCollectionNames() {
        // Arrange
        MongoConfig config = new MongoConfig();
        
        // Assert
        assertEquals("user_recommendations", config.getRecommendationsCollection());
        assertEquals("analytics", config.getAnalyticsCollection());
    }
    
    @Test
    void testSetRecommendationsCollection() {
        // Arrange
        MongoConfig config = new MongoConfig();
        String newCollectionName = "custom_recommendations";
        
        // Act
        config.setRecommendationsCollection(newCollectionName);
        
        // Assert
        assertEquals(newCollectionName, config.getRecommendationsCollection());
    }
    
    @Test
    void testSetAnalyticsCollection() {
        // Arrange
        MongoConfig config = new MongoConfig();
        String newCollectionName = "custom_analytics";
        
        // Act
        config.setAnalyticsCollection(newCollectionName);
        
        // Assert
        assertEquals(newCollectionName, config.getAnalyticsCollection());
    }
}