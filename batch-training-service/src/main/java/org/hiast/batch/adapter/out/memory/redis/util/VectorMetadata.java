package org.hiast.batch.adapter.out.memory.redis.util;

import java.time.Instant;
import java.util.Objects;

/**
 * Metadata container for vectors stored in Redis.
 * This class encapsulates additional information about vectors that can be used
 * for vector search operations, indexing, and management.
 */
public class VectorMetadata {
    private final int entityId;
    private final String entityType;
    private final int dimension;
    private final Instant timestamp;
    private final String modelVersion;
    
    /**
     * Constructs vector metadata.
     * 
     * @param entityId The ID of the entity (user or item)
     * @param entityType The type of entity ("user" or "item")
     * @param dimension The dimension of the vector
     * @param timestamp The timestamp when the vector was created/updated
     * @param modelVersion The version of the model that generated this vector
     */
    public VectorMetadata(int entityId, String entityType, int dimension, 
                         Instant timestamp, String modelVersion) {
        this.entityId = entityId;
        this.entityType = Objects.requireNonNull(entityType, "Entity type cannot be null");
        this.dimension = dimension;
        this.timestamp = Objects.requireNonNull(timestamp, "Timestamp cannot be null");
        this.modelVersion = modelVersion;
        
        if (dimension <= 0) {
            throw new IllegalArgumentException("Dimension must be positive");
        }
    }
    
    /**
     * Creates metadata for a user vector.
     * 
     * @param userId The user ID
     * @param dimension The vector dimension
     * @param modelVersion The model version
     * @return VectorMetadata instance
     */
    public static VectorMetadata forUser(int userId, int dimension, String modelVersion) {
        return new VectorMetadata(userId, VectorSerializationUtil.USER_ENTITY_TYPE, 
                                 dimension, Instant.now(), modelVersion);
    }
    
    /**
     * Creates metadata for an item vector.
     * 
     * @param itemId The item ID
     * @param dimension The vector dimension
     * @param modelVersion The model version
     * @return VectorMetadata instance
     */
    public static VectorMetadata forItem(int itemId, int dimension, String modelVersion) {
        return new VectorMetadata(itemId, VectorSerializationUtil.ITEM_ENTITY_TYPE, 
                                 dimension, Instant.now(), modelVersion);
    }
    
    /**
     * Gets the entity ID.
     * 
     * @return The entity ID
     */
    public int getEntityId() {
        return entityId;
    }
    
    /**
     * Gets the entity type.
     * 
     * @return The entity type
     */
    public String getEntityType() {
        return entityType;
    }
    
    /**
     * Gets the vector dimension.
     * 
     * @return The dimension
     */
    public int getDimension() {
        return dimension;
    }
    
    /**
     * Gets the timestamp.
     * 
     * @return The timestamp
     */
    public Instant getTimestamp() {
        return timestamp;
    }
    
    /**
     * Gets the model version.
     * 
     * @return The model version
     */
    public String getModelVersion() {
        return modelVersion;
    }
    
    /**
     * Checks if this is a user vector.
     * 
     * @return true if it's a user vector
     */
    public boolean isUserVector() {
        return VectorSerializationUtil.USER_ENTITY_TYPE.equals(entityType);
    }
    
    /**
     * Checks if this is an item vector.
     * 
     * @return true if it's an item vector
     */
    public boolean isItemVector() {
        return VectorSerializationUtil.ITEM_ENTITY_TYPE.equals(entityType);
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        VectorMetadata that = (VectorMetadata) o;
        return entityId == that.entityId &&
               dimension == that.dimension &&
               Objects.equals(entityType, that.entityType) &&
               Objects.equals(timestamp, that.timestamp) &&
               Objects.equals(modelVersion, that.modelVersion);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(entityId, entityType, dimension, timestamp, modelVersion);
    }
    
    @Override
    public String toString() {
        return "VectorMetadata{" +
               "entityId=" + entityId +
               ", entityType='" + entityType + '\'' +
               ", dimension=" + dimension +
               ", timestamp=" + timestamp +
               ", modelVersion='" + modelVersion + '\'' +
               '}';
    }
}
