package org.hiast.recommendationsapi.application.service;

import org.hiast.model.MovieRecommendation;
import org.hiast.model.UserRecommendations;
import org.hiast.recommendationsapi.application.port.out.RecommendationsRepositoryPort;
import org.hiast.recommendationsapi.domain.exception.InvalidRecommendationRequestException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
        int userId = 1;
        Instant now = Instant.now();

        List<MovieRecommendation> movieRecommendations = Arrays.asList(
            mock(MovieRecommendation.class),
            mock(MovieRecommendation.class)
        );

        UserRecommendations expectedRecommendations = mock(UserRecommendations.class);
        when(expectedRecommendations.getRecommendations()).thenReturn(movieRecommendations);

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
        int userId = 1;
        when(recommendationsRepository.findByUserId(userId))
            .thenReturn(Optional.empty());

        // When
        Optional<UserRecommendations> result = recommendationsService.getUserRecommendations(userId);

        // Then
        assertFalse(result.isPresent());
        verify(recommendationsRepository).findByUserId(userId);
    }

    @Test
    void getUserRecommendations_WithInvalidUserId_ShouldThrowException() {
        // When & Then
        assertThrows(InvalidRecommendationRequestException.class, () ->
            recommendationsService.getUserRecommendations(0));

        assertThrows(InvalidRecommendationRequestException.class, () ->
            recommendationsService.getUserRecommendations(-1));

        verifyNoInteractions(recommendationsRepository);
    }

    @Test
    void getUserRecommendationsWithLimit_WhenValidLimit_ShouldReturnLimitedRecommendations() {
        // Given
        int userId = 1;
        int limit = 5;
        Instant now = Instant.now();

        List<MovieRecommendation> movieRecommendations = Arrays.asList(
            mock(MovieRecommendation.class),
            mock(MovieRecommendation.class)
        );

        UserRecommendations expectedRecommendations = mock(UserRecommendations.class);
        when(expectedRecommendations.getRecommendations()).thenReturn(movieRecommendations);

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
        int userId = 1;

        // When & Then
        assertThrows(InvalidRecommendationRequestException.class, () ->
            recommendationsService.getUserRecommendations(userId, 0));

        assertThrows(InvalidRecommendationRequestException.class, () ->
            recommendationsService.getUserRecommendations(userId, -1));

        verifyNoInteractions(recommendationsRepository);
    }

    @Test
    void getBatchUserRecommendations_WhenRecommendationsExist_ShouldReturnRecommendations() {
        // Given
        List<Integer> userIds = Arrays.asList(1, 2, 3);
        Map<Integer, UserRecommendations> expectedRecommendations = new HashMap<>();
        expectedRecommendations.put(1, mock(UserRecommendations.class));
        expectedRecommendations.put(2, mock(UserRecommendations.class));

        when(recommendationsRepository.findByUserIds(userIds))
            .thenReturn(expectedRecommendations);

        // When
        Map<Integer, UserRecommendations> result = recommendationsService.getBatchUserRecommendations(userIds);

        // Then
        assertEquals(2, result.size());
        assertTrue(result.containsKey(1));
        assertTrue(result.containsKey(2));
        verify(recommendationsRepository).findByUserIds(userIds);
    }

    @Test
    void getBatchUserRecommendations_WithEmptyList_ShouldThrowException() {
        // Given
        List<Integer> userIds = List.of();

        // When & Then
        assertThrows(InvalidRecommendationRequestException.class, () ->
            recommendationsService.getBatchUserRecommendations(userIds));

        verifyNoInteractions(recommendationsRepository);
    }

    @Test
    void getBatchUserRecommendationsWithLimit_WhenValidLimit_ShouldReturnLimitedRecommendations() {
        // Given
        List<Integer> userIds = Arrays.asList(1, 2, 3);
        int limit = 5;
        Map<Integer, UserRecommendations> expectedRecommendations = new HashMap<>();
        expectedRecommendations.put(1, mock(UserRecommendations.class));
        expectedRecommendations.put(2, mock(UserRecommendations.class));

        when(recommendationsRepository.findByUserIdsWithLimit(userIds, limit))
            .thenReturn(expectedRecommendations);

        // When
        Map<Integer, UserRecommendations> result = recommendationsService.getBatchUserRecommendations(userIds, limit);

        // Then
        assertEquals(2, result.size());
        assertTrue(result.containsKey(1));
        assertTrue(result.containsKey(2));
        verify(recommendationsRepository).findByUserIdsWithLimit(userIds, limit);
    }
}
