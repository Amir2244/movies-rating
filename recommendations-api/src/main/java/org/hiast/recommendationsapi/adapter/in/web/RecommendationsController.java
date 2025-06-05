package org.hiast.recommendationsapi.adapter.in.web;

import org.hiast.ids.UserId;
import org.hiast.recommendationsapi.adapter.in.web.response.UserRecommendationsResponse;
import org.hiast.recommendationsapi.adapter.in.web.mapper.RecommendationsResponseMapper;
import org.hiast.recommendationsapi.application.port.in.GetUserRecommendationsUseCase;
import org.hiast.recommendationsapi.domain.model.UserRecommendations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.Objects;
import java.util.Optional;

/**
 * REST controller for recommendations API endpoints.
 * This is the primary adapter in hexagonal architecture that handles
 * HTTP requests and delegates to the application layer.
 */
@RestController
@RequestMapping("/recommendations")
@CrossOrigin(origins = "*")
public class RecommendationsController {
    
    private static final Logger log = LoggerFactory.getLogger(RecommendationsController.class);
    
    private final GetUserRecommendationsUseCase getUserRecommendationsUseCase;
    private final RecommendationsResponseMapper responseMapper;
    
    /**
     * Constructor for dependency injection.
     *
     * @param getUserRecommendationsUseCase The use case for retrieving user recommendations.
     * @param responseMapper                The mapper for converting domain models to response DTOs.
     */
    public RecommendationsController(GetUserRecommendationsUseCase getUserRecommendationsUseCase,
                                   RecommendationsResponseMapper responseMapper) {
        this.getUserRecommendationsUseCase = Objects.requireNonNull(getUserRecommendationsUseCase, 
            "getUserRecommendationsUseCase cannot be null");
        this.responseMapper = Objects.requireNonNull(responseMapper, 
            "responseMapper cannot be null");
    }
    
    /**
     * Retrieves recommendations for a specific user.
     *
     * @param userId The user ID to get recommendations for.
     * @return ResponseEntity containing user recommendations or 404 if not found.
     */
    @GetMapping("/users/{userId}")
    public ResponseEntity<UserRecommendationsResponse> getUserRecommendations(@PathVariable int userId) {
        log.info("Received request for recommendations for user: {}", userId);
        
        try {
            // Validate user ID
            if (userId <= 0) {
                log.warn("Invalid user ID: {}", userId);
                return ResponseEntity.badRequest().build();
            }
            
            UserId userIdObj = UserId.of(userId);
            Optional<UserRecommendations> recommendations = getUserRecommendationsUseCase.getUserRecommendations(userIdObj);
            
            if (recommendations.isPresent()) {
                UserRecommendationsResponse response = responseMapper.toResponse(recommendations.get());
                log.info("Successfully retrieved {} recommendations for user: {}", 
                    response.getTotalRecommendations(), userId);
                return ResponseEntity.ok(response);
            } else {
                log.info("No recommendations found for user: {}", userId);
                return ResponseEntity.notFound().build();
            }
        } catch (IllegalArgumentException e) {
            log.warn("Invalid request for user {}: {}", userId, e.getMessage());
            return ResponseEntity.badRequest().build();
        } catch (Exception e) {
            log.error("Error retrieving recommendations for user: {}", userId, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Retrieves a limited number of recommendations for a specific user.
     *
     * @param userId The user ID to get recommendations for.
     * @param limit  Maximum number of recommendations to return.
     * @return ResponseEntity containing user recommendations or 404 if not found.
     */
    @GetMapping("/users/{userId}/top/{limit}")
    public ResponseEntity<UserRecommendationsResponse> getUserRecommendationsWithLimit(
            @PathVariable int userId, 
            @PathVariable int limit) {
        log.info("Received request for top {} recommendations for user: {}", limit, userId);
        
        try {
            // Validate parameters
            if (userId <= 0) {
                log.warn("Invalid user ID: {}", userId);
                return ResponseEntity.badRequest().build();
            }
            
            if (limit <= 0 || limit > 10) { // Reasonable upper limit
                log.warn("Invalid limit: {} (must be between 1 and 100)", limit);
                return ResponseEntity.badRequest().build();
            }
            
            UserId userIdObj = UserId.of(userId);
            Optional<UserRecommendations> recommendations = 
                getUserRecommendationsUseCase.getUserRecommendations(userIdObj, limit);
            
            if (recommendations.isPresent()) {
                UserRecommendationsResponse response = responseMapper.toResponse(recommendations.get());
                log.info("Successfully retrieved {} recommendations for user: {} (limit: {})", 
                    response.getTotalRecommendations(), userId, limit);
                return ResponseEntity.ok(response);
            } else {
                log.info("No recommendations found for user: {}", userId);
                return ResponseEntity.notFound().build();
            }
        } catch (IllegalArgumentException e) {
            log.warn("Invalid request for user {} with limit {}: {}", userId, limit, e.getMessage());
            return ResponseEntity.badRequest().build();
        } catch (Exception e) {
            log.error("Error retrieving recommendations for user: {} with limit: {}", userId, limit, e);
            return ResponseEntity.internalServerError().build();
        }
    }
}
