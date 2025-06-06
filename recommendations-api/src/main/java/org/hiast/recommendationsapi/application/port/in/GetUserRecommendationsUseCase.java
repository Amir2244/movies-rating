package org.hiast.recommendationsapi.application.port.in;

import org.hiast.model.UserRecommendations;

import java.util.Optional;

/**
 * Input port (use case) for retrieving user recommendations.
 * This defines the contract for the primary business operation.
 * Uses primitive int for user IDs for consistency with storage layer.
 */
public interface GetUserRecommendationsUseCase {
    
    /**
     * Retrieves recommendations for a specific user.
     *
     * @param userId The user ID to get recommendations for.
     * @return Optional containing user recommendations if found, empty otherwise.
     */
    Optional<UserRecommendations> getUserRecommendations(int userId);
    
    /**
     * Retrieves recommendations for a specific user with a limit.
     *
     * @param userId The user ID to get recommendations for.
     * @param limit  Maximum number of recommendations to return.
     * @return Optional containing user recommendations if found, empty otherwise.
     */
    Optional<UserRecommendations> getUserRecommendations(int userId, int limit);
}
