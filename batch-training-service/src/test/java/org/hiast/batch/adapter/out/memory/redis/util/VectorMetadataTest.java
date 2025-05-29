package org.hiast.batch.adapter.out.memory.redis.util;

import org.hiast.batch.util.VectorMetadata;
import org.hiast.batch.util.VectorSerializationUtil;
import org.junit.Test;

import java.time.Instant;

import static org.junit.Assert.*;

/**
 * Unit tests for VectorMetadata.
 */
public class VectorMetadataTest {

    @Test
    public void testCreateUserMetadata() {
        // Arrange
        int userId = 123;
        int dimension = 50;
        String modelVersion = "test-v1.0";
        
        // Act
        VectorMetadata metadata = VectorMetadata.forUser(userId, dimension, modelVersion);
        
        // Assert
        assertEquals(userId, metadata.getEntityId());
        assertEquals(VectorSerializationUtil.USER_ENTITY_TYPE, metadata.getEntityType());
        assertEquals(dimension, metadata.getDimension());
        assertEquals(modelVersion, metadata.getModelVersion());
        assertTrue(metadata.isUserVector());
        assertFalse(metadata.isItemVector());
        assertNotNull(metadata.getTimestamp());
    }
    
    @Test
    public void testCreateItemMetadata() {
        // Arrange
        int itemId = 456;
        int dimension = 100;
        String modelVersion = "test-v2.0";
        
        // Act
        VectorMetadata metadata = VectorMetadata.forItem(itemId, dimension, modelVersion);
        
        // Assert
        assertEquals(itemId, metadata.getEntityId());
        assertEquals(VectorSerializationUtil.ITEM_ENTITY_TYPE, metadata.getEntityType());
        assertEquals(dimension, metadata.getDimension());
        assertEquals(modelVersion, metadata.getModelVersion());
        assertFalse(metadata.isUserVector());
        assertTrue(metadata.isItemVector());
        assertNotNull(metadata.getTimestamp());
    }
    
    @Test
    public void testConstructorWithAllParameters() {
        // Arrange
        int entityId = 789;
        String entityType = "custom";
        int dimension = 25;
        Instant timestamp = Instant.now();
        String modelVersion = "custom-v1.0";
        
        // Act
        VectorMetadata metadata = new VectorMetadata(entityId, entityType, dimension, timestamp, modelVersion);
        
        // Assert
        assertEquals(entityId, metadata.getEntityId());
        assertEquals(entityType, metadata.getEntityType());
        assertEquals(dimension, metadata.getDimension());
        assertEquals(timestamp, metadata.getTimestamp());
        assertEquals(modelVersion, metadata.getModelVersion());
    }
    
    @Test(expected = NullPointerException.class)
    public void testConstructorWithNullEntityType() {
        new VectorMetadata(123, null, 50, Instant.now(), "v1.0");
    }
    
    @Test(expected = NullPointerException.class)
    public void testConstructorWithNullTimestamp() {
        new VectorMetadata(123, "user", 50, null, "v1.0");
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithZeroDimension() {
        new VectorMetadata(123, "user", 0, Instant.now(), "v1.0");
    }
    
    @Test(expected = IllegalArgumentException.class)
    public void testConstructorWithNegativeDimension() {
        new VectorMetadata(123, "user", -5, Instant.now(), "v1.0");
    }
    
    @Test
    public void testEqualsAndHashCode() {
        // Arrange
        Instant timestamp = Instant.now();
        VectorMetadata metadata1 = new VectorMetadata(123, "user", 50, timestamp, "v1.0");
        VectorMetadata metadata2 = new VectorMetadata(123, "user", 50, timestamp, "v1.0");
        VectorMetadata metadata3 = new VectorMetadata(456, "user", 50, timestamp, "v1.0");
        
        // Assert
        assertEquals(metadata1, metadata2);
        assertEquals(metadata1.hashCode(), metadata2.hashCode());
        assertNotEquals(metadata1, metadata3);
        assertNotEquals(metadata1.hashCode(), metadata3.hashCode());
        
        // Test with null
        assertNotEquals(metadata1, null);
        
        // Test with different class
        assertNotEquals(metadata1, "not a metadata object");
    }
    
    @Test
    public void testToString() {
        // Arrange
        Instant timestamp = Instant.parse("2023-01-01T12:00:00Z");
        VectorMetadata metadata = new VectorMetadata(123, "user", 50, timestamp, "v1.0");
        
        // Act
        String result = metadata.toString();
        
        // Assert
        assertTrue(result.contains("entityId=123"));
        assertTrue(result.contains("entityType='user'"));
        assertTrue(result.contains("dimension=50"));
        assertTrue(result.contains("modelVersion='v1.0'"));
        assertTrue(result.contains("timestamp=2023-01-01T12:00:00Z"));
    }
    
    @Test
    public void testTimestampIsRecent() {
        // Arrange
        Instant before = Instant.now();
        
        // Act
        VectorMetadata metadata = VectorMetadata.forUser(123, 50, "v1.0");
        
        // Assert
        Instant after = Instant.now();
        assertTrue("Timestamp should be between before and after", 
                  !metadata.getTimestamp().isBefore(before) && 
                  !metadata.getTimestamp().isAfter(after));
    }
}
