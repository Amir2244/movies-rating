package org.hiast.batch.adapter.out.memory.redis.util;

import org.hiast.batch.util.VectorSerializationException;
import org.hiast.batch.util.VectorSerializationUtil;
import org.junit.Test;

import static org.junit.Assert.*;

/**
 * Unit tests for VectorSerializationUtil.
 */
public class VectorSerializationUtilTest {

    @Test
    public void testSerializeAndDeserializeVector() {
        // Arrange
        float[] originalVector = {1.0f, 2.5f, -3.7f, 0.0f, 100.123f};
        
        // Act
        byte[] serialized = VectorSerializationUtil.serializeVector(originalVector);
        float[] deserialized = VectorSerializationUtil.deserializeVector(serialized);
        
        // Assert
        assertNotNull(serialized);
        assertNotNull(deserialized);
        assertEquals(originalVector.length, deserialized.length);
        assertArrayEquals(originalVector, deserialized, 0.0001f);
    }
    
    @Test
    public void testCalculateByteSize() {
        // Arrange & Act & Assert
        assertEquals(20, VectorSerializationUtil.calculateByteSize(5)); // 5 floats * 4 bytes
        assertEquals(40, VectorSerializationUtil.calculateByteSize(10)); // 10 floats * 4 bytes
        assertEquals(0, VectorSerializationUtil.calculateByteSize(0));
    }
    
    @Test
    public void testCalculateDimension() {
        // Arrange
        float[] vector = {1.0f, 2.0f, 3.0f};
        byte[] serialized = VectorSerializationUtil.serializeVector(vector);
        
        // Act & Assert
        assertEquals(3, VectorSerializationUtil.calculateDimension(serialized));
        assertEquals(0, VectorSerializationUtil.calculateDimension(null));
    }
    
    @Test
    public void testCreateKeys() {
        // Act & Assert
        assertEquals("vector:user:123", VectorSerializationUtil.createUserFactorKey(123));
        assertEquals("vector:item:456", VectorSerializationUtil.createItemFactorKey(456));
    }
    
    @Test
    public void testExtractEntityId() {
        // Act & Assert
        assertEquals(123, VectorSerializationUtil.extractEntityId("vector:user:123"));
        assertEquals(456, VectorSerializationUtil.extractEntityId("vector:item:456"));
        assertEquals(-1, VectorSerializationUtil.extractEntityId("invalid:key"));
        assertEquals(-1, VectorSerializationUtil.extractEntityId(null));
    }
    
    @Test
    public void testKeyTypeChecks() {
        // Act & Assert
        assertTrue(VectorSerializationUtil.isUserVectorKey("vector:user:123"));
        assertFalse(VectorSerializationUtil.isUserVectorKey("vector:item:123"));
        assertFalse(VectorSerializationUtil.isUserVectorKey(null));
        
        assertTrue(VectorSerializationUtil.isItemVectorKey("vector:item:456"));
        assertFalse(VectorSerializationUtil.isItemVectorKey("vector:user:456"));
        assertFalse(VectorSerializationUtil.isItemVectorKey(null));
    }
    
    @Test(expected = VectorSerializationException.class)
    public void testSerializeNullVector() {
        VectorSerializationUtil.serializeVector(null);
    }
    
    @Test(expected = VectorSerializationException.class)
    public void testSerializeEmptyVector() {
        VectorSerializationUtil.serializeVector(new float[0]);
    }
    
    @Test(expected = VectorSerializationException.class)
    public void testSerializeVectorWithNaN() {
        float[] vector = {1.0f, Float.NaN, 3.0f};
        VectorSerializationUtil.serializeVector(vector);
    }
    
    @Test(expected = VectorSerializationException.class)
    public void testSerializeVectorWithInfinity() {
        float[] vector = {1.0f, Float.POSITIVE_INFINITY, 3.0f};
        VectorSerializationUtil.serializeVector(vector);
    }
    
    @Test(expected = VectorSerializationException.class)
    public void testDeserializeNullBytes() {
        VectorSerializationUtil.deserializeVector(null);
    }
    
    @Test(expected = VectorSerializationException.class)
    public void testDeserializeEmptyBytes() {
        VectorSerializationUtil.deserializeVector(new byte[0]);
    }
    
    @Test(expected = VectorSerializationException.class)
    public void testDeserializeInvalidLength() {
        // 3 bytes is not divisible by 4 (float size)
        VectorSerializationUtil.deserializeVector(new byte[3]);
    }
    
    @Test
    public void testValidateVector() {
        // Valid vectors should not throw
        VectorSerializationUtil.validateVector(new float[]{1.0f, 2.0f, 3.0f});
        VectorSerializationUtil.validateVector(new float[]{0.0f, -1.0f, 100.5f});
        
        // Invalid vectors should throw
        try {
            VectorSerializationUtil.validateVector(null);
            fail("Should throw exception for null vector");
        } catch (VectorSerializationException e) {
            // Expected
        }
        
        try {
            VectorSerializationUtil.validateVector(new float[0]);
            fail("Should throw exception for empty vector");
        } catch (VectorSerializationException e) {
            // Expected
        }
        
        try {
            VectorSerializationUtil.validateVector(new float[]{1.0f, Float.NaN});
            fail("Should throw exception for NaN value");
        } catch (VectorSerializationException e) {
            // Expected
        }
    }
    
    @Test
    public void testLargeVector() {
        // Test with a larger vector
        float[] largeVector = new float[1000];
        for (int i = 0; i < largeVector.length; i++) {
            largeVector[i] = (float) Math.sin(i * 0.1);
        }
        
        byte[] serialized = VectorSerializationUtil.serializeVector(largeVector);
        float[] deserialized = VectorSerializationUtil.deserializeVector(serialized);
        
        assertEquals(largeVector.length, deserialized.length);
        assertArrayEquals(largeVector, deserialized, 0.0001f);
        assertEquals(4000, serialized.length); // 1000 floats * 4 bytes
    }
}
