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
    public void testSerializeVector() {
        // Arrange
        float[] originalVector = {1.0f, 2.5f, -3.7f, 0.0f, 100.123f};
        
        // Act
        byte[] serialized = VectorSerializationUtil.serializeVector(originalVector);
        
        // Assert
        assertNotNull(serialized);
        assertEquals(originalVector.length * 4, serialized.length); // 4 bytes per float
    }
    
    @Test
    public void testCreateKeys() {
        // Act & Assert
        assertEquals("vector:user:123", VectorSerializationUtil.createUserFactorKey(123));
        assertEquals("vector:item:456", VectorSerializationUtil.createItemFactorKey(456));
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
    public void testLargeVectorSerialization() {
        // Test with a larger vector
        float[] largeVector = new float[1000];
        for (int i = 0; i < largeVector.length; i++) {
            largeVector[i] = (float) Math.sin(i * 0.1);
        }
        
        byte[] serialized = VectorSerializationUtil.serializeVector(largeVector);
        
        assertEquals(largeVector.length * 4, serialized.length); // 1000 floats * 4 bytes
    }
}
