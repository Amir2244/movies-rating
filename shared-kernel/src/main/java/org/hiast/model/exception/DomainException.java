package org.hiast.model.exception;

/**
 * Base exception for all domain-related errors in the shared kernel.
 * This provides a common exception hierarchy that can be used across
 * all services in the movie recommendation system.
 */
public abstract class DomainException extends RuntimeException {
    
    protected DomainException(String message) {
        super(message);
    }
    
    protected DomainException(String message, Throwable cause) {
        super(message, cause);
    }
} 