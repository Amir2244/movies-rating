package org.hiast.recommendationsapi.adapter.out.persistence.mongodb.mapper;

import org.hiast.ids.MovieId;
import org.hiast.ids.UserId;
import org.hiast.recommendationsapi.adapter.out.persistence.mongodb.document.MovieRecommendationDocument;
import org.hiast.recommendationsapi.adapter.out.persistence.mongodb.document.UserRecommendationsDocument;
import org.hiast.model.MovieRecommendation;
import org.hiast.model.MovieMetaData;
import org.hiast.model.UserRecommendations;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.stream.Collectors;

/**
 * Mapper component for converting between MongoDB documents and domain models.
 * This follows the hexagonal architecture principle of keeping domain models
 * separate from persistence concerns.
 */
@Component
public class RecommendationsDomainMapper {
    
    /**
     * Converts a MongoDB document to a domain model.
     *
     * @param document The MongoDB document to convert.
     * @return The corresponding domain model.
     */
    public UserRecommendations toDomain(UserRecommendationsDocument document) {
        if (document == null) {
            return null;
        }
        
        UserId userId = UserId.of(document.getUserId());
        
        List<MovieRecommendation> recommendations = document.getRecommendations()
                .stream()
                .map(recDoc -> toDomainWithUserId(recDoc, userId))
                .collect(Collectors.toList());
        
        return new UserRecommendations(
                userId,
                recommendations,
                document.getGeneratedAt(),
                document.getModelVersion()
        );
    }
    
    /**
     * Converts a MongoDB movie recommendation document to a domain model.
     *
     * @param document The MongoDB document to convert.
     * @return The corresponding domain model.
     */
    private MovieRecommendation toDomain(MovieRecommendationDocument document) {
        if (document == null) {
            return null;
        }
        
        // Note: We need to get the userId from the parent document context
        // For now, we'll create a placeholder - this will be set correctly in the calling method
        UserId userId = UserId.of(1); // This will be overridden
        MovieId movieId = MovieId.of(document.getMovieId());
        
        // Create MovieMetaData from document fields
        MovieMetaData movieMetaData = new MovieMetaData(movieId, document.getTitle(), document.getGenres());
        
        return new MovieRecommendation(
                userId,
                movieId,
                document.getRating(),
                document.getGeneratedAt(),
                movieMetaData
        );
    }
    
    /**
     * Converts a MongoDB document to a domain model with limited recommendations.
     *
     * @param document The MongoDB document to convert.
     * @param limit    Maximum number of recommendations to include.
     * @return The corresponding domain model with limited recommendations.
     */
    public UserRecommendations toDomainWithLimit(UserRecommendationsDocument document, int limit) {
        if (document == null) {
            return null;
        }
        
        UserId userId = UserId.of(document.getUserId());
        
        List<MovieRecommendation> recommendations = document.getRecommendations()
                .stream()
                .limit(limit)
                .map(recDoc -> toDomainWithUserId(recDoc, userId))
                .collect(Collectors.toList());
        
        return new UserRecommendations(
                userId,
                recommendations,
                document.getGeneratedAt(),
                document.getModelVersion()
        );
    }
    
    /**
     * Converts a MongoDB movie recommendation document to a domain model with the correct userId.
     *
     * @param document The MongoDB document to convert.
     * @param userId   The user ID to use in the domain model.
     * @return The corresponding domain model.
     */
    private MovieRecommendation toDomainWithUserId(MovieRecommendationDocument document, UserId userId) {
        if (document == null) {
            return null;
        }
        
        MovieId movieId = MovieId.of(document.getMovieId());
        
        // Create MovieMetaData from document fields
        MovieMetaData movieMetaData = new MovieMetaData(movieId, document.getTitle(), document.getGenres());
        
        return new MovieRecommendation(
                userId,
                movieId,
                document.getRating(),
                document.getGeneratedAt(),
                movieMetaData
        );
    }
}
