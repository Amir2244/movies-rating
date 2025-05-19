package org.hiast.batch.application.port.out;

import org.hiast.batch.domain.model.UserRecommendations;

import java.util.List;

/**
 * Output Port defining how to persist the recommendation results.
 * Implementations (adapters) will handle specific persistence stores like MongoDB.
 */
public interface ResultPersistencePort {
    /**
     * Saves user recommendations to the persistence store.
     *
     * @param userRecommendations List of user recommendations to save.
     * @return True if the save operation was successful, false otherwise.
     */
    boolean saveUserRecommendations(List<UserRecommendations> userRecommendations);
}
