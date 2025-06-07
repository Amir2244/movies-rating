package org.hiast.batch.application.port.out;

import org.hiast.model.UserRecommendations;

import java.io.Serializable;
import java.util.List;

/**
 * Output Port defining how to persist the recommendation results.
 * Implementations (adapters) will handle specific persistence stores like MongoDB.
 * Extends Serializable to support Spark distributed operations.
 */
public interface ResultPersistencePort extends Serializable {
    /**
     * Saves user recommendations to the persistence store.
     *
     * @param userRecommendations List of user recommendations to save.
     * @return True if the save operation was successful, false otherwise.
     */
    boolean saveUserRecommendations(List<UserRecommendations> userRecommendations);
}
