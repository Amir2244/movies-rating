package org.hiast.recommendationsapi.domain.exception;

/**
 * Domain exception thrown when a recommendation request is invalid.
 */
public class InvalidRecommendationRequestException extends RecommendationException {
    
    public InvalidRecommendationRequestException(String message) {
        super(message);
    }
    
    public InvalidRecommendationRequestException(String message, Throwable cause) {
        super(message, cause);
    }
} 