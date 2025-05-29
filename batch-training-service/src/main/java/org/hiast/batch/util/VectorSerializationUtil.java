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
        // Utility class - prevent instantiation
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
     * Deserializes a byte array back to a float array.
     * 
     * @param bytes The byte array to deserialize
     * @return The deserialized float array
     * @throws VectorSerializationException if deserialization fails
     */
    public static float[] deserializeVector(byte[] bytes) {
        if (bytes == null) {
            throw new VectorSerializationException("Byte array cannot be null");
        }
        
        if (bytes.length == 0) {
            throw new VectorSerializationException("Byte array cannot be empty");
        }
        
        if (bytes.length % FLOAT_SIZE_BYTES != 0) {
            throw new VectorSerializationException(
                "Invalid byte array length: " + bytes.length + 
                ". Must be divisible by " + FLOAT_SIZE_BYTES);
        }
        
        try {
            ByteBuffer buffer = ByteBuffer.wrap(bytes);
            buffer.order(BYTE_ORDER);
            
            int dimension = bytes.length / FLOAT_SIZE_BYTES;
            float[] vector = new float[dimension];
            
            for (int i = 0; i < dimension; i++) {
                vector[i] = buffer.getFloat();
            }
            
            log.debug("Deserialized {} bytes to vector of dimension {}", 
                     bytes.length, dimension);
            return vector;
            
        } catch (Exception e) {
            throw new VectorSerializationException(
                "Failed to deserialize byte array of length " + bytes.length, e);
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
     * Calculates the byte size required to store a vector of given dimension.
     * 
     * @param dimension The vector dimension
     * @return The required byte size
     */
    public static int calculateByteSize(int dimension) {
        return dimension * FLOAT_SIZE_BYTES;
    }
    
    /**
     * Calculates the dimension of a vector from its byte representation.
     * 
     * @param bytes The byte array
     * @return The vector dimension
     */
    public static int calculateDimension(byte[] bytes) {
        if (bytes == null) {
            return 0;
        }
        return bytes.length / FLOAT_SIZE_BYTES;
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
    
    /**
     * Extracts entity ID from a vector key.
     * 
     * @param key The Redis key
     * @return The entity ID, or -1 if extraction fails
     */
    public static int extractEntityId(String key) {
        if (key == null || key.isEmpty()) {
            return -1;
        }
        
        try {
            String[] parts = key.split(":");
            if (parts.length >= 3) {
                return Integer.parseInt(parts[2]);
            }
        } catch (NumberFormatException e) {
            log.warn("Failed to extract entity ID from key: {}", key);
        }
        
        return -1;
    }
    
    /**
     * Determines if a key represents a user vector.
     * 
     * @param key The Redis key
     * @return true if it's a user vector key
     */
    public static boolean isUserVectorKey(String key) {
        return key != null && key.startsWith("vector:user:");
    }
    
    /**
     * Determines if a key represents an item vector.
     * 
     * @param key The Redis key
     * @return true if it's an item vector key
     */
    public static boolean isItemVectorKey(String key) {
        return key != null && key.startsWith("vector:item:");
    }
}
