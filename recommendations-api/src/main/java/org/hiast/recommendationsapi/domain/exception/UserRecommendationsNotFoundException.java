package org.hiast.recommendationsapi.domain.exception;

/**
 * Domain exception thrown when user recommendations are not found.
 * Uses primitive int for user ID for consistency with storage layer.
 */
public class UserRecommendationsNotFoundException extends RecommendationException {
    
    private final int userId;
    
    public UserRecommendationsNotFoundException(int userId) {
        super("No recommendations found for user: " + userId);
        this.userId = userId;
    }
    
    public int getUserId() {
        return userId;
    }
} 