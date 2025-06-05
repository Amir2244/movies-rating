package org.hiast.recommendationsapi.application.port.out;

import org.hiast.ids.UserId;
import org.hiast.model.UserRecommendations;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Output port for accessing recommendation data.
 * This defines the contract for data persistence operations.
 */
public interface RecommendationsRepositoryPort {
    
    /**
     * Finds user recommendations by user ID.
     *
     * @param userId The user ID to search for.
     * @return Optional containing user recommendations if found, empty otherwise.
     */
    Optional<UserRecommendations> findByUserId(UserId userId);
    
    /**
     * Finds user recommendations by user ID with a limit on the number of recommendations.
     *
     * @param userId The user ID to search for.
     * @param limit  Maximum number of recommendations to return.
     * @return Optional containing user recommendations if found, empty otherwise.
     */
    Optional<UserRecommendations> findByUserIdWithLimit(UserId userId, int limit);
    
    /**
     * Checks if recommendations exist for a user.
     *
     * @param userId The user ID to check.
     * @return true if recommendations exist, false otherwise.
     */
    boolean existsByUserId(UserId userId);
    
    /**
     * Finds recommendations for multiple users.
     *
     * @param userIds The list of user IDs to search for.
     * @return Map of user ID to their recommendations (only includes users with recommendations).
     */
    Map<UserId, UserRecommendations> findByUserIds(List<UserId> userIds);
    
    /**
     * Finds limited recommendations for multiple users.
     *
     * @param userIds The list of user IDs to search for.
     * @param limit   Maximum number of recommendations per user.
     * @return Map of user ID to their recommendations (only includes users with recommendations).
     */
    Map<UserId, UserRecommendations> findByUserIdsWithLimit(List<UserId> userIds, int limit);
}
