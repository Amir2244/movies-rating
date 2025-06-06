package org.hiast.recommendationsapi.application.service;

import org.hiast.model.UserRecommendations;
import org.hiast.recommendationsapi.application.port.in.GetBatchUserRecommendationsUseCase;
import org.hiast.recommendationsapi.application.port.in.GetUserRecommendationsUseCase;
import org.hiast.recommendationsapi.application.port.out.RecommendationsRepositoryPort;
import org.hiast.recommendationsapi.config.CacheConfig;
import org.hiast.recommendationsapi.domain.exception.InvalidRecommendationRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Service implementing user recommendations use cases.
 * This is the application service that orchestrates business logic.
 * Uses primitive int for user IDs for consistency with storage layer.
 */
@Service
public class RecommendationsService implements GetUserRecommendationsUseCase, GetBatchUserRecommendationsUseCase {
    
    private static final Logger log = LoggerFactory.getLogger(RecommendationsService.class);
    
    private final RecommendationsRepositoryPort recommendationsRepository;
    
    /**
     * Constructor for dependency injection.
     *
     * @param recommendationsRepository The repository port for accessing recommendation data.
     */
    public RecommendationsService(RecommendationsRepositoryPort recommendationsRepository) {
        this.recommendationsRepository = Objects.requireNonNull(recommendationsRepository, 
            "recommendationsRepository cannot be null");
    }
    
    @Override
    @Cacheable(value = CacheConfig.USER_RECOMMENDATIONS_CACHE, key = "#userId")
    public Optional<UserRecommendations> getUserRecommendations(int userId) {
        if (userId <= 0) {
            throw new InvalidRecommendationRequestException("User ID must be positive, but was: " + userId);
        }

        log.debug("Retrieving recommendations for user: {}", userId);
        
        Optional<UserRecommendations> recommendations = recommendationsRepository.findByUserId(userId);
        
        if (recommendations.isPresent()) {
            log.info("Found {} recommendations for user: {}", 
                recommendations.get().getRecommendations().size(), userId);
        } else {
            log.info("No recommendations found for user: {}", userId);
        }
        
        return recommendations;
    }
    
    @Override
    @Cacheable(value = CacheConfig.USER_RECOMMENDATIONS_LIMITED_CACHE, key = "#userId + ':' + #limit")
    public Optional<UserRecommendations> getUserRecommendations(int userId, int limit) {
        if (userId <= 0) {
            throw new InvalidRecommendationRequestException("User ID must be positive, but was: " + userId);
        }
        
        if (limit <= 0) {
            throw new InvalidRecommendationRequestException("Limit must be positive, but was: " + limit);
        }

        log.debug("Retrieving up to {} recommendations for user: {}", limit, userId);
        
        Optional<UserRecommendations> recommendations = recommendationsRepository.findByUserIdWithLimit(userId, limit);
        
        if (recommendations.isPresent()) {
            log.info("Found {} recommendations for user: {} (limit: {})", 
                recommendations.get().getRecommendations().size(), userId, limit);
        } else {
            log.info("No recommendations found for user: {}", userId);
        }
        
        return recommendations;
    }
    
    @Override
    public Map<Integer, UserRecommendations> getBatchUserRecommendations(List<Integer> userIds) {
        Objects.requireNonNull(userIds, "userIds cannot be null");
        
        if (userIds.isEmpty()) {
            throw new InvalidRecommendationRequestException("User IDs list cannot be empty");
        }
        
        if (userIds.size() > 100) { // Reasonable batch limit
            throw new InvalidRecommendationRequestException("Too many user IDs. Maximum 100 allowed, but got: " + userIds.size());
        }

        log.debug("Retrieving recommendations for {} users", userIds.size());
        
        Map<Integer, UserRecommendations> recommendations = recommendationsRepository.findByUserIds(userIds);
        
        log.info("Found recommendations for {} out of {} requested users", 
            recommendations.size(), userIds.size());
        
        return recommendations;
    }
    
    @Override
    public Map<Integer, UserRecommendations> getBatchUserRecommendations(List<Integer> userIds, int limit) {
        Objects.requireNonNull(userIds, "userIds cannot be null");
        
        if (userIds.isEmpty()) {
            throw new InvalidRecommendationRequestException("User IDs list cannot be empty");
        }
        
        if (userIds.size() > 100) { // Reasonable batch limit
            throw new InvalidRecommendationRequestException("Too many user IDs. Maximum 100 allowed, but got: " + userIds.size());
        }
        
        if (limit <= 0) {
            throw new InvalidRecommendationRequestException("Limit must be positive, but was: " + limit);
        }

        log.debug("Retrieving up to {} recommendations for {} users", limit, userIds.size());
        
        Map<Integer, UserRecommendations> recommendations = recommendationsRepository.findByUserIdsWithLimit(userIds, limit);
        
        log.info("Found recommendations for {} out of {} requested users (limit: {})", 
            recommendations.size(), userIds.size(), limit);
        
        return recommendations;
    }
}
