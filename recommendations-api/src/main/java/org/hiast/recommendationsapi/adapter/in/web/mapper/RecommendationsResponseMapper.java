package org.hiast.recommendationsapi.adapter.in.web.mapper;

import org.hiast.recommendationsapi.adapter.in.web.response.MovieRecommendationResponse;
import org.hiast.recommendationsapi.adapter.in.web.response.UserRecommendationsResponse;
import org.hiast.model.MovieRecommendation;
import org.hiast.model.UserRecommendations;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Mapper component for converting domain models to API response DTOs.
 * This follows the hexagonal architecture principle of keeping domain models
 * separate from API representation concerns.
 */
@Component
public class RecommendationsResponseMapper {
    
    /**
     * Converts a domain model to an API response DTO.
     *
     * @param domain The domain model to convert.
     * @return The corresponding API response DTO.
     */
    public UserRecommendationsResponse toResponse(UserRecommendations domain) {
        if (domain == null) {
            return null;
        }
        
        List<MovieRecommendationResponse> recommendationResponses = domain.getRecommendations()
                .stream()
                .map(this::toResponse)
                .collect(Collectors.toList());
        
        return new UserRecommendationsResponse(
                domain.getUserId().getUserId(),
                recommendationResponses,
                domain.getGeneratedAt(),
                domain.getModelVersion()
        );
    }
    
    /**
     * Converts a movie recommendation domain model to an API response DTO.
     *
     * @param domain The domain model to convert.
     * @return The corresponding API response DTO.
     */
    private MovieRecommendationResponse toResponse(MovieRecommendation domain) {
        if (domain == null) {
            return null;
        }
        
        return new MovieRecommendationResponse(
                domain.getMovieId().getMovieId(),
                domain.getPredictedRating(),
                domain.getGeneratedAt(),
                domain.getMovieTitle(),
                domain.getMovieGenres()
        );
    }
}
