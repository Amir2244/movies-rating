package org.hiast.recommendationsapi.application.port.in;

import org.hiast.model.UserRecommendations;

import java.util.List;
import java.util.Map;

/**
 * Input port (use case) for retrieving recommendations for multiple users in batch.
 * This is useful for dashboard views or analytics that need multiple user data.
 * Uses primitive int for user IDs for consistency with storage layer.
 */
public interface GetBatchUserRecommendationsUseCase {
    
    /**
     * Retrieves recommendations for multiple users.
     *
     * @param userIds The list of user IDs to get recommendations for.
     * @return Map of user ID to their recommendations (only includes users with recommendations).
     */
    Map<Integer, UserRecommendations> getBatchUserRecommendations(List<Integer> userIds);
    
    /**
     * Retrieves limited recommendations for multiple users.
     *
     * @param userIds The list of user IDs to get recommendations for.
     * @param limit   Maximum number of recommendations per user.
     * @return Map of user ID to their recommendations (only includes users with recommendations).
     */
    Map<Integer, UserRecommendations> getBatchUserRecommendations(List<Integer> userIds, int limit);
} 