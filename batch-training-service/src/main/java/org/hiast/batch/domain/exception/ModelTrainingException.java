package org.hiast.batch.domain.exception;

/**
 * Exception thrown when there is an error during model training.
 */
public class ModelTrainingException extends BatchTrainingException {
    
    /**
     * Constructs a new model training exception with the specified detail message.
     *
     * @param message the detail message
     */
    public ModelTrainingException(String message) {
        super(message);
    }
    
    /**
     * Constructs a new model training exception with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause the cause
     */
    public ModelTrainingException(String message, Throwable cause) {
        super(message, cause);
    }
}
