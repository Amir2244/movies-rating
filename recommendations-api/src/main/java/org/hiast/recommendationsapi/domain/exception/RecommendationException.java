package org.hiast.recommendationsapi.domain.exception;

/**
 * Base exception for all recommendation-related domain errors.
 * This follows domain-driven design principles by keeping business
 * exceptions in the domain layer.
 */
public abstract class RecommendationException extends RuntimeException {
    
    protected RecommendationException(String message) {
        super(message);
    }
    
    protected RecommendationException(String message, Throwable cause) {
        super(message, cause);
    }
} 