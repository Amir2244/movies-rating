package org.hiast.batch.util;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;

/**
 * Utility class for serializing and deserializing float vectors to/from byte arrays
 * for Redis vector database storage. This enables Redis to treat stored factor data
 * as vectors for similarity search and recommendation queries.
 * 
 * The serialization format is optimized for Redis Stack/RedisSearch vector operations:
 * - Uses little-endian byte order for compatibility
 * - Stores vectors as compact byte arrays
 * - Includes dimension metadata for vector search indexing
 */
public final class VectorSerializationUtil {
    private static final Logger log = LoggerFactory.getLogger(VectorSerializationUtil.class);
    
    // Constants for vector serialization
    private static final int FLOAT_SIZE_BYTES = 4;
    private static final ByteOrder BYTE_ORDER = ByteOrder.LITTLE_ENDIAN;
    
    // Metadata constants for Redis vector storage
    public static final String VECTOR_FIELD = "vector";
    public static final String DIMENSION_FIELD = "dimension";
    public static final String ENTITY_TYPE_FIELD = "entity_type";
    public static final String ENTITY_ID_FIELD = "entity_id";
    public static final String TIMESTAMP_FIELD = "timestamp";
    
    // Entity types for vector classification
    public static final String USER_ENTITY_TYPE = "user";
    public static final String ITEM_ENTITY_TYPE = "item";
    
    private VectorSerializationUtil() {
    }
    
    /**
     * Serializes a float array to a byte array for Redis storage.
     * Uses little-endian byte order for compatibility with Redis vector operations.
     * 
     * @param vector The float array to serialize
     * @return The serialized byte array
     * @throws VectorSerializationException if serialization fails
     */
    public static byte[] serializeVector(float[] vector) {
        if (vector == null) {
            throw new VectorSerializationException("Vector cannot be null");
        }
        
        if (vector.length == 0) {
            throw new VectorSerializationException("Vector cannot be empty");
        }
        
        try {
            ByteBuffer buffer = ByteBuffer.allocate(vector.length * FLOAT_SIZE_BYTES);
            buffer.order(BYTE_ORDER);
            
            for (float value : vector) {
                if (!Float.isFinite(value)) {
                    throw new VectorSerializationException(
                        "Vector contains non-finite value: " + value);
                }
                buffer.putFloat(value);
            }
            
            byte[] result = buffer.array();
            log.debug("Serialized vector of dimension {} to {} bytes", 
                     vector.length, result.length);
            return result;
            
        } catch (Exception e) {
            throw new VectorSerializationException(
                "Failed to serialize vector of dimension " + vector.length, e);
        }
    }
    
    /**
     * Validates that a vector is suitable for Redis vector storage.
     * 
     * @param vector The vector to validate
     * @throws VectorSerializationException if validation fails
     */
    public static void validateVector(float[] vector) {
        if (vector == null) {
            throw new VectorSerializationException("Vector cannot be null");
        }
        
        if (vector.length == 0) {
            throw new VectorSerializationException("Vector cannot be empty");
        }
        
        for (int i = 0; i < vector.length; i++) {
            float value = vector[i];
            if (!Float.isFinite(value)) {
                throw new VectorSerializationException(
                    "Vector contains non-finite value at index " + i + ": " + value);
            }
        }
    }
    
    /**
     * Creates a Redis key for storing user factors with vector database compatibility.
     * 
     * @param userId The user ID
     * @return The Redis key
     */
    public static String createUserFactorKey(int userId) {
        return "vector:user:" + userId;
    }
    
    /**
     * Creates a Redis key for storing item factors with vector database compatibility.
     * 
     * @param itemId The item ID
     * @return The Redis key
     */
    public static String createItemFactorKey(int itemId) {
        return "vector:item:" + itemId;
    }
}
