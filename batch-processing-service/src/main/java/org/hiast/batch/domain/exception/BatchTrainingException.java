package org.hiast.batch.domain.exception;

/**
 * Base exception class for all batch training service exceptions.
 * This provides a common type for catching and handling all service-specific exceptions.
 */
public class BatchTrainingException extends RuntimeException {
    
    /**
     * Constructs a new batch training exception with the specified detail message.
     *
     * @param message the detail message
     */
    public BatchTrainingException(String message) {
        super(message);
    }
    
    /**
     * Constructs a new batch training exception with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause the cause
     */
    public BatchTrainingException(String message, Throwable cause) {
        super(message, cause);
    }
}
