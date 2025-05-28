package org.hiast.batch.adapter.out.memory.redis.util;

/**
 * Exception thrown when vector serialization or deserialization operations fail.
 * This exception is used to wrap and provide context for errors that occur during
 * the conversion between float arrays and byte arrays for Redis vector storage.
 */
public class VectorSerializationException extends RuntimeException {
    private static final long serialVersionUID = 1L;
    
    /**
     * Constructs a new VectorSerializationException with the specified detail message.
     * 
     * @param message the detail message
     */
    public VectorSerializationException(String message) {
        super(message);
    }
    
    /**
     * Constructs a new VectorSerializationException with the specified detail message and cause.
     * 
     * @param message the detail message
     * @param cause the cause of the exception
     */
    public VectorSerializationException(String message, Throwable cause) {
        super(message, cause);
    }
    
    /**
     * Constructs a new VectorSerializationException with the specified cause.
     * 
     * @param cause the cause of the exception
     */
    public VectorSerializationException(Throwable cause) {
        super(cause);
    }
}
