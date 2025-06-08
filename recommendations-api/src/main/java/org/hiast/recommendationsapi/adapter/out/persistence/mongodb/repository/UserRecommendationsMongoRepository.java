package org.hiast.recommendationsapi.adapter.out.persistence.mongodb.repository;

import org.hiast.recommendationsapi.adapter.out.persistence.mongodb.document.UserRecommendationsDocument;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.List;
import java.util.Optional;

/**
 * Spring Data MongoDB repository for UserRecommendationsDocument.
 * This provides the basic CRUD operations and custom queries.
 */
@Repository
public interface UserRecommendationsMongoRepository extends MongoRepository<UserRecommendationsDocument, String> {
    
    /**
     * Finds user recommendations by user ID.
     *
     * @param userId The user ID to search for.
     * @return Optional containing the document if found.
     */
    Optional<UserRecommendationsDocument> findByUserId(int userId);
    
    /**
     * Checks if recommendations exist for a user.
     *
     * @param userId The user ID to check.
     * @return true if recommendations exist, false otherwise.
     */
    boolean existsByUserId(int userId);
    
    /**
     * Finds user recommendations by user ID and limits the number of recommendations returned.
     * This uses MongoDB aggregation to slice the recommendations array.
     *
     * @param userId The user ID to search for.
     * @param limit  Maximum number of recommendations to return.
     * @return Optional containing the document with limited recommendations if found.
     */
    @Query("{ 'userId': ?0 }")
    Optional<UserRecommendationsDocument> findByUserIdWithLimit(int userId, int limit);
    
    /**
     * Finds user recommendations for multiple user IDs.
     *
     * @param userIds The list of user IDs to search for.
     * @return List of documents for users with recommendations.
     */
    List<UserRecommendationsDocument> findByUserIdIn(List<Integer> userIds);
}
