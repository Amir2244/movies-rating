package org.hiast.recommendationsapi.application.port.in;

import org.hiast.ids.UserId;
import org.hiast.model.UserRecommendations;

import java.util.List;
import java.util.Map;

/**
 * Input port (use case) for retrieving recommendations for multiple users in batch.
 * This is useful for dashboard views or analytics that need multiple user data.
 */
public interface GetBatchUserRecommendationsUseCase {
    
    /**
     * Retrieves recommendations for multiple users.
     *
     * @param userIds The list of user IDs to get recommendations for.
     * @return Map of user ID to their recommendations (only includes users with recommendations).
     */
    Map<UserId, UserRecommendations> getBatchUserRecommendations(List<UserId> userIds);
    
    /**
     * Retrieves limited recommendations for multiple users.
     *
     * @param userIds The list of user IDs to get recommendations for.
     * @param limit   Maximum number of recommendations per user.
     * @return Map of user ID to their recommendations (only includes users with recommendations).
     */
    Map<UserId, UserRecommendations> getBatchUserRecommendations(List<UserId> userIds, int limit);
} 