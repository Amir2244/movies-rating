package org.hiast.recommendationsapi.application.service;

import org.hiast.ids.UserId;
import org.hiast.recommendationsapi.application.port.in.GetUserRecommendationsUseCase;
import org.hiast.recommendationsapi.application.port.out.RecommendationsRepositoryPort;
import org.hiast.recommendationsapi.domain.model.UserRecommendations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.Objects;
import java.util.Optional;

/**
 * Application service implementing the GetUserRecommendationsUseCase.
 * This is the core business logic layer that orchestrates the recommendation retrieval process.
 */
@Service
public class RecommendationsService implements GetUserRecommendationsUseCase {
    
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
    public Optional<UserRecommendations> getUserRecommendations(UserId userId, int limit) {
        Objects.requireNonNull(userId, "userId cannot be null");
        
        if (limit <= 0) {
            throw new IllegalArgumentException("Limit must be positive, but was: " + limit);
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
}
