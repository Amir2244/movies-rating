package org.hiast.recommendationsapi.adapter.in.web;

import org.hiast.recommendationsapi.adapter.in.web.response.UserRecommendationsResponse;
import org.hiast.recommendationsapi.adapter.in.web.mapper.RecommendationsResponseMapper;
import org.hiast.recommendationsapi.application.port.in.GetUserRecommendationsUseCase;
import org.hiast.recommendationsapi.application.port.in.GetBatchUserRecommendationsUseCase;
import org.hiast.recommendationsapi.domain.exception.InvalidRecommendationRequestException;
import org.hiast.recommendationsapi.domain.exception.UserRecommendationsNotFoundException;
import org.hiast.model.UserRecommendations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * REST controller for recommendations API endpoints.
 * This is the primary adapter in hexagonal architecture that handles
 * HTTP requests and delegates to the application layer.
 * Uses primitive int for user IDs for consistency with storage layer.
 */
@RestController
@RequestMapping("/recommendations")
@CrossOrigin(origins = "*")
public class RecommendationsController {
    
    private static final Logger log = LoggerFactory.getLogger(RecommendationsController.class);
    
    private final GetUserRecommendationsUseCase getUserRecommendationsUseCase;
    private final GetBatchUserRecommendationsUseCase getBatchUserRecommendationsUseCase;
    private final RecommendationsResponseMapper responseMapper;
    
    /**
     * Constructor for dependency injection.
     *
     * @param getUserRecommendationsUseCase      The use case for retrieving user recommendations.
     * @param getBatchUserRecommendationsUseCase The use case for retrieving batch user recommendations.
     * @param responseMapper                     The mapper for converting domain models to response DTOs.
     */
    public RecommendationsController(GetUserRecommendationsUseCase getUserRecommendationsUseCase,
                                   GetBatchUserRecommendationsUseCase getBatchUserRecommendationsUseCase,
                                   RecommendationsResponseMapper responseMapper) {
        this.getUserRecommendationsUseCase = Objects.requireNonNull(getUserRecommendationsUseCase, 
            "getUserRecommendationsUseCase cannot be null");
        this.getBatchUserRecommendationsUseCase = Objects.requireNonNull(getBatchUserRecommendationsUseCase,
            "getBatchUserRecommendationsUseCase cannot be null");
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
            
            Optional<UserRecommendations> recommendations = getUserRecommendationsUseCase.getUserRecommendations(userId);
            
            if (recommendations.isPresent()) {
                UserRecommendationsResponse response = responseMapper.toResponse(recommendations.get());
                log.info("Successfully retrieved {} recommendations for user: {}", 
                    response.getTotalRecommendations(), userId);
                return ResponseEntity.ok(response);
            } else {
                log.info("No recommendations found for user: {}", userId);
                return ResponseEntity.notFound().build();
            }
        } catch (InvalidRecommendationRequestException e) {
            log.warn("Invalid request for user {}: {}", userId, e.getMessage());
            return ResponseEntity.badRequest().build();
        } catch (UserRecommendationsNotFoundException e) {
            log.info("No recommendations found for user: {}", userId);
            return ResponseEntity.notFound().build();
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
            
            Optional<UserRecommendations> recommendations = 
                getUserRecommendationsUseCase.getUserRecommendations(userId, limit);
            
            if (recommendations.isPresent()) {
                UserRecommendationsResponse response = responseMapper.toResponse(recommendations.get());
                log.info("Successfully retrieved {} recommendations for user: {} (limit: {})", 
                    response.getTotalRecommendations(), userId, limit);
                return ResponseEntity.ok(response);
            } else {
                log.info("No recommendations found for user: {}", userId);
                return ResponseEntity.notFound().build();
            }
        } catch (InvalidRecommendationRequestException e) {
            log.warn("Invalid request for user {} with limit {}: {}", userId, limit, e.getMessage());
            return ResponseEntity.badRequest().build();
        } catch (UserRecommendationsNotFoundException e) {
            log.info("No recommendations found for user: {} with limit: {}", userId, limit);
            return ResponseEntity.notFound().build();
        } catch (Exception e) {
            log.error("Error retrieving recommendations for user: {} with limit: {}", userId, limit, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Retrieves recommendations for multiple users in batch.
     *
     * @param userIds Comma-separated list of user IDs.
     * @return ResponseEntity containing a map of user recommendations.
     */
    @GetMapping("/batch")
    public ResponseEntity<Map<String, UserRecommendationsResponse>> getBatchUserRecommendations(
            @RequestParam("userIds") String userIds) {
        log.info("Received batch request for user IDs: {}", userIds);
        
        try {
            List<Integer> userIdList = parseUserIds(userIds);
            
            Map<Integer, UserRecommendations> recommendations = 
                getBatchUserRecommendationsUseCase.getBatchUserRecommendations(userIdList);
            
            Map<String, UserRecommendationsResponse> response = convertBatchResponse(recommendations);
            
            log.info("Successfully retrieved batch recommendations for {} out of {} users", 
                response.size(), userIdList.size());
            return ResponseEntity.ok(response);
            
        } catch (InvalidRecommendationRequestException e) {
            log.warn("Invalid batch request: {}", e.getMessage());
            return ResponseEntity.badRequest().build();
        } catch (Exception e) {
            log.error("Error retrieving batch recommendations for user IDs: {}", userIds, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Retrieves limited recommendations for multiple users in batch.
     *
     * @param userIds Comma-separated list of user IDs.
     * @param limit   Maximum number of recommendations per user.
     * @return ResponseEntity containing a map of user recommendations.
     */
    @GetMapping("/batch/top/{limit}")
    public ResponseEntity<Map<String, UserRecommendationsResponse>> getBatchUserRecommendationsWithLimit(
            @RequestParam("userIds") String userIds,
            @PathVariable int limit) {
        log.info("Received batch request for top {} recommendations for user IDs: {}", limit, userIds);
        
        try {
            List<Integer> userIdList = parseUserIds(userIds);
            
            Map<Integer, UserRecommendations> recommendations = 
                getBatchUserRecommendationsUseCase.getBatchUserRecommendations(userIdList, limit);
            
            Map<String, UserRecommendationsResponse> response = convertBatchResponse(recommendations);
            
            log.info("Successfully retrieved batch recommendations for {} out of {} users (limit: {})", 
                response.size(), userIdList.size(), limit);
            return ResponseEntity.ok(response);
            
        } catch (InvalidRecommendationRequestException e) {
            log.warn("Invalid batch request with limit {}: {}", limit, e.getMessage());
            return ResponseEntity.badRequest().build();
        } catch (Exception e) {
            log.error("Error retrieving batch recommendations for user IDs: {} with limit: {}", userIds, limit, e);
            return ResponseEntity.internalServerError().build();
        }
    }
    
    /**
     * Parses comma-separated user IDs string into a list of Integer objects.
     */
    private List<Integer> parseUserIds(String userIdsString) {
        try {
            return Stream.of(userIdsString.split(","))
                .map(String::trim)
                .map(Integer::parseInt)
                .collect(Collectors.toList());
        } catch (NumberFormatException e) {
            throw new InvalidRecommendationRequestException("Invalid user ID format: " + userIdsString);
        }
    }
    
    /**
     * Converts batch domain results to response DTOs.
     */
    private Map<String, UserRecommendationsResponse> convertBatchResponse(
            Map<Integer, UserRecommendations> recommendations) {
        Map<String, UserRecommendationsResponse> response = new HashMap<>();
        for (Map.Entry<Integer, UserRecommendations> entry : recommendations.entrySet()) {
            String userIdKey = String.valueOf(entry.getKey());
            UserRecommendationsResponse userResponse = responseMapper.toResponse(entry.getValue());
            response.put(userIdKey, userResponse);
        }
        return response;
    }
}
