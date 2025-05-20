package org.hiast.batch.domain.exception;

/**
 * Exception thrown when there is an error loading data from the data source.
 */
public class DataLoadingException extends BatchTrainingException {
    
    /**
     * Constructs a new data loading exception with the specified detail message.
     *
     * @param message the detail message
     */
    public DataLoadingException(String message) {
        super(message);
    }
    
    /**
     * Constructs a new data loading exception with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause the cause
     */
    public DataLoadingException(String message, Throwable cause) {
        super(message, cause);
    }
}
