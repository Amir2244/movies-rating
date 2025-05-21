package org.hiast.batch.domain.exception;

/**
 * Exception thrown when there is an error in the configuration.
 */
public class ConfigurationException extends BatchTrainingException {
    
    /**
     * Constructs a new configuration exception with the specified detail message.
     *
     * @param message the detail message
     */
    public ConfigurationException(String message) {
        super(message);
    }
    
    /**
     * Constructs a new configuration exception with the specified detail message and cause.
     *
     * @param message the detail message
     * @param cause the cause
     */
    public ConfigurationException(String message, Throwable cause) {
        super(message, cause);
    }
}
