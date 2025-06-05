package org.hiast.recommendationsapi.application.service;

import org.hiast.ids.UserId;
import org.hiast.recommendationsapi.application.port.in.GetUserRecommendationsUseCase;
import org.hiast.recommendationsapi.application.port.in.GetBatchUserRecommendationsUseCase;
import org.hiast.recommendationsapi.application.port.out.RecommendationsRepositoryPort;
import org.hiast.recommendationsapi.aspect.annotation.Cacheable;
import org.hiast.recommendationsapi.aspect.annotation.Monitored;
import org.hiast.recommendationsapi.aspect.annotation.Validated;
import org.hiast.recommendationsapi.config.CacheConfig;
import org.hiast.recommendationsapi.domain.exception.InvalidRecommendationRequestException;
import org.hiast.recommendationsapi.domain.exception.UserRecommendationsNotFoundException;
import org.hiast.model.UserRecommendations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;

/**
 * Application service implementing recommendation use cases.
 * This is the core business logic layer that orchestrates the recommendation retrieval process.
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
    @Cacheable(value = CacheConfig.USER_RECOMMENDATIONS_CACHE, key = "#userId.userId", ttl = 3600)
    @Monitored(value = "getUserRecommendations")
    @Validated
    public Optional<UserRecommendations> getUserRecommendations(UserId userId) {
        Objects.requireNonNull(userId, "userId cannot be null");
        
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
    @Cacheable(value = CacheConfig.USER_RECOMMENDATIONS_LIMITED_CACHE, key = "#userId.userId + ':' + #limit", ttl = 1800)
    @Monitored(value = "getUserRecommendationsWithLimit")
    @Validated
    public Optional<UserRecommendations> getUserRecommendations(UserId userId, int limit) {
        Objects.requireNonNull(userId, "userId cannot be null");
        
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
    @Monitored(value = "getBatchUserRecommendations", thresholdMs = 2000)
    @Validated
    public Map<UserId, UserRecommendations> getBatchUserRecommendations(List<UserId> userIds) {
        Objects.requireNonNull(userIds, "userIds cannot be null");
        
        if (userIds.isEmpty()) {
            throw new InvalidRecommendationRequestException("User IDs list cannot be empty");
        }
        
        if (userIds.size() > 100) { // Reasonable batch limit
            throw new InvalidRecommendationRequestException("Batch size cannot exceed 100 users");
        }
        
        log.debug("Retrieving recommendations for {} users", userIds.size());
        
        Map<UserId, UserRecommendations> recommendations = recommendationsRepository.findByUserIds(userIds);
        
        log.info("Found recommendations for {} out of {} requested users", 
            recommendations.size(), userIds.size());
        
        return recommendations;
    }
    
    @Override
    @Monitored(value = "getBatchUserRecommendationsWithLimit", thresholdMs = 2000)
    @Validated
    public Map<UserId, UserRecommendations> getBatchUserRecommendations(List<UserId> userIds, int limit) {
        Objects.requireNonNull(userIds, "userIds cannot be null");
        
        if (userIds.isEmpty()) {
            throw new InvalidRecommendationRequestException("User IDs list cannot be empty");
        }
        
        if (userIds.size() > 100) { // Reasonable batch limit
            throw new InvalidRecommendationRequestException("Batch size cannot exceed 100 users");
        }
        
        if (limit <= 0) {
            throw new InvalidRecommendationRequestException("Limit must be positive, but was: " + limit);
        }
        
        log.debug("Retrieving up to {} recommendations for {} users", limit, userIds.size());
        
        Map<UserId, UserRecommendations> recommendations = recommendationsRepository.findByUserIdsWithLimit(userIds, limit);
        
        log.info("Found recommendations for {} out of {} requested users (limit: {})", 
            recommendations.size(), userIds.size(), limit);
        
        return recommendations;
    }
}
