package org.hiast.batch.domain.exception;

/**
 * Exception thrown when there is an error persisting the model or its factors.
 */
public class ModelPersistenceException extends BatchTrainingException {
    
    /**
     * Constructs a new model persistence exception with the specified detail message.
     *
     * @param message the detail message
     */
    public ModelPersistenceException(String message) {
        super(message);
    }
    
    /**
     * Constructs a new model persistence exception with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause the cause
     */
    public ModelPersistenceException(String message, Throwable cause) {
        super(message, cause);
    }
}
