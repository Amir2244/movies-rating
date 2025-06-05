package org.hiast.recommendationsapi.domain.exception;

import org.hiast.ids.UserId;

/**
 * Domain exception thrown when user recommendations are not found.
 */
public class UserRecommendationsNotFoundException extends RecommendationException {
    
    private final UserId userId;
    
    public UserRecommendationsNotFoundException(UserId userId) {
        super("No recommendations found for user: " + userId);
        this.userId = userId;
    }
    
    public UserId getUserId() {
        return userId;
    }
} 