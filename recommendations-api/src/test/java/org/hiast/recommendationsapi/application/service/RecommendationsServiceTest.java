package org.hiast.recommendationsapi.application.service;

import org.hiast.ids.MovieId;
import org.hiast.ids.UserId;
import org.hiast.recommendationsapi.application.port.out.RecommendationsRepositoryPort;
import org.hiast.recommendationsapi.domain.model.MovieRecommendation;
import org.hiast.recommendationsapi.domain.model.UserRecommendations;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for RecommendationsService.
 */
@ExtendWith(MockitoExtension.class)
class RecommendationsServiceTest {
    
    @Mock
    private RecommendationsRepositoryPort recommendationsRepository;
    
    private RecommendationsService recommendationsService;
    
    @BeforeEach
    void setUp() {
        recommendationsService = new RecommendationsService(recommendationsRepository);
    }
    
    @Test
    void getUserRecommendations_WhenRecommendationsExist_ShouldReturnRecommendations() {
        // Given
        UserId userId = UserId.of(1);
        Instant now = Instant.now();
        
        List<MovieRecommendation> movieRecommendations = Arrays.asList(
            new MovieRecommendation(userId, MovieId.of(100), 4.5f, now),
            new MovieRecommendation(userId, MovieId.of(200), 4.2f, now)
        );
        
        UserRecommendations expectedRecommendations = new UserRecommendations(
            userId, movieRecommendations, now, "test-model-v1"
        );
        
        when(recommendationsRepository.findByUserId(userId))
            .thenReturn(Optional.of(expectedRecommendations));
        
        // When
        Optional<UserRecommendations> result = recommendationsService.getUserRecommendations(userId);
        
        // Then
        assertTrue(result.isPresent());
        assertEquals(expectedRecommendations, result.get());
        assertEquals(2, result.get().getRecommendations().size());
        verify(recommendationsRepository).findByUserId(userId);
    }
    
    @Test
    void getUserRecommendations_WhenNoRecommendationsExist_ShouldReturnEmpty() {
        // Given
        UserId userId = UserId.of(1);
        when(recommendationsRepository.findByUserId(userId))
            .thenReturn(Optional.empty());
        
        // When
        Optional<UserRecommendations> result = recommendationsService.getUserRecommendations(userId);
        
        // Then
        assertFalse(result.isPresent());
        verify(recommendationsRepository).findByUserId(userId);
    }
    
    @Test
    void getUserRecommendations_WithNullUserId_ShouldThrowException() {
        // When & Then
        assertThrows(NullPointerException.class, () -> 
            recommendationsService.getUserRecommendations(null));
        
        verifyNoInteractions(recommendationsRepository);
    }
    
    @Test
    void getUserRecommendationsWithLimit_WhenValidLimit_ShouldReturnLimitedRecommendations() {
        // Given
        UserId userId = UserId.of(1);
        int limit = 5;
        Instant now = Instant.now();
        
        List<MovieRecommendation> movieRecommendations = Arrays.asList(
            new MovieRecommendation(userId, MovieId.of(100), 4.5f, now),
            new MovieRecommendation(userId, MovieId.of(200), 4.2f, now)
        );
        
        UserRecommendations expectedRecommendations = new UserRecommendations(
            userId, movieRecommendations, now, "test-model-v1"
        );
        
        when(recommendationsRepository.findByUserIdWithLimit(userId, limit))
            .thenReturn(Optional.of(expectedRecommendations));
        
        // When
        Optional<UserRecommendations> result = recommendationsService.getUserRecommendations(userId, limit);
        
        // Then
        assertTrue(result.isPresent());
        assertEquals(expectedRecommendations, result.get());
        verify(recommendationsRepository).findByUserIdWithLimit(userId, limit);
    }
    
    @Test
    void getUserRecommendationsWithLimit_WithInvalidLimit_ShouldThrowException() {
        // Given
        UserId userId = UserId.of(1);
        
        // When & Then
        assertThrows(IllegalArgumentException.class, () -> 
            recommendationsService.getUserRecommendations(userId, 0));
        
        assertThrows(IllegalArgumentException.class, () -> 
            recommendationsService.getUserRecommendations(userId, -1));
        
        verifyNoInteractions(recommendationsRepository);
    }
}
