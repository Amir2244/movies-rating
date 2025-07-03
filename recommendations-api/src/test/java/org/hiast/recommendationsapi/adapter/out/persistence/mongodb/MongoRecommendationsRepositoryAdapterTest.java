package org.hiast.recommendationsapi.adapter.out.persistence.mongodb;

import org.hiast.model.MovieRecommendation;
import org.hiast.model.UserRecommendations;
import org.hiast.recommendationsapi.adapter.out.persistence.mongodb.document.UserRecommendationsDocument;
import org.hiast.recommendationsapi.adapter.out.persistence.mongodb.mapper.RecommendationsDomainMapper;
import org.hiast.recommendationsapi.adapter.out.persistence.mongodb.repository.UserRecommendationsMongoRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for MongoRecommendationsRepositoryAdapter.
 */
@ExtendWith(MockitoExtension.class)
class MongoRecommendationsRepositoryAdapterTest {

    @Mock
    private UserRecommendationsMongoRepository mongoRepository;

    @Mock
    private RecommendationsDomainMapper domainMapper;

    private MongoRecommendationsRepositoryAdapter adapter;

    @BeforeEach
    void setUp() {
        adapter = new MongoRecommendationsRepositoryAdapter(mongoRepository, domainMapper);
    }

    @Test
    void findByUserId_WhenRecommendationsExist_ShouldReturnRecommendations() {
        // Given
        int userId = 1;
        UserRecommendationsDocument document = mock(UserRecommendationsDocument.class);
        UserRecommendations userRecommendations = mock(UserRecommendations.class);

        when(mongoRepository.findByUserId(userId)).thenReturn(Optional.of(document));
        when(domainMapper.toDomain(document)).thenReturn(userRecommendations);

        // When
        Optional<UserRecommendations> result = adapter.findByUserId(userId);

        // Then
        assertTrue(result.isPresent());
        assertEquals(userRecommendations, result.get());
        verify(mongoRepository).findByUserId(userId);
        verify(domainMapper).toDomain(document);
    }

    @Test
    void findByUserId_WhenNoRecommendationsExist_ShouldReturnEmpty() {
        // Given
        int userId = 1;
        when(mongoRepository.findByUserId(userId)).thenReturn(Optional.empty());

        // When
        Optional<UserRecommendations> result = adapter.findByUserId(userId);

        // Then
        assertFalse(result.isPresent());
        verify(mongoRepository).findByUserId(userId);
        verifyNoInteractions(domainMapper);
    }

    @Test
    void findByUserIdWithLimit_WhenRecommendationsExist_ShouldReturnLimitedRecommendations() {
        // Given
        int userId = 1;
        int limit = 5;
        UserRecommendationsDocument document = mock(UserRecommendationsDocument.class);
        UserRecommendations userRecommendations = mock(UserRecommendations.class);

        when(mongoRepository.findByUserId(userId)).thenReturn(Optional.of(document));
        when(domainMapper.toDomainWithLimit(document, limit)).thenReturn(userRecommendations);

        // When
        Optional<UserRecommendations> result = adapter.findByUserIdWithLimit(userId, limit);

        // Then
        assertTrue(result.isPresent());
        assertEquals(userRecommendations, result.get());
        verify(mongoRepository).findByUserId(userId);
        verify(domainMapper).toDomainWithLimit(document, limit);
    }

    @Test
    void existsByUserId_WhenRecommendationsExist_ShouldReturnTrue() {
        // Given
        int userId = 1;
        when(mongoRepository.existsByUserId(userId)).thenReturn(true);

        // When
        boolean result = adapter.existsByUserId(userId);

        // Then
        assertTrue(result);
        verify(mongoRepository).existsByUserId(userId);
    }

    @Test
    void existsByUserId_WhenNoRecommendationsExist_ShouldReturnFalse() {
        // Given
        int userId = 1;
        when(mongoRepository.existsByUserId(userId)).thenReturn(false);

        // When
        boolean result = adapter.existsByUserId(userId);

        // Then
        assertFalse(result);
        verify(mongoRepository).existsByUserId(userId);
    }

    @Test
    void findByUserIds_WhenRecommendationsExist_ShouldReturnRecommendations() {
        // Given
        List<Integer> userIds = Arrays.asList(1, 2, 3);
        List<UserRecommendationsDocument> documents = Arrays.asList(
            createUserRecommendationsDocument(1),
            createUserRecommendationsDocument(2)
        );
        
        when(mongoRepository.findByUserIdIn(userIds)).thenReturn(documents);
        when(domainMapper.toDomain(any(UserRecommendationsDocument.class)))
            .thenReturn(mock(UserRecommendations.class));

        // When
        Map<Integer, UserRecommendations> result = adapter.findByUserIds(userIds);

        // Then
        assertEquals(2, result.size());
        assertTrue(result.containsKey(1));
        assertTrue(result.containsKey(2));
        verify(mongoRepository).findByUserIdIn(userIds);
        verify(domainMapper, times(2)).toDomain(any(UserRecommendationsDocument.class));
    }

    @Test
    void findByUserIdsWithLimit_WhenRecommendationsExist_ShouldReturnLimitedRecommendations() {
        // Given
        List<Integer> userIds = Arrays.asList(1, 2, 3);
        int limit = 5;
        List<UserRecommendationsDocument> documents = Arrays.asList(
            createUserRecommendationsDocument(1),
            createUserRecommendationsDocument(2)
        );
        
        when(mongoRepository.findByUserIdIn(userIds)).thenReturn(documents);
        when(domainMapper.toDomainWithLimit(any(UserRecommendationsDocument.class), eq(limit)))
            .thenReturn(mock(UserRecommendations.class));

        // When
        Map<Integer, UserRecommendations> result = adapter.findByUserIdsWithLimit(userIds, limit);

        // Then
        assertEquals(2, result.size());
        assertTrue(result.containsKey(1));
        assertTrue(result.containsKey(2));
        verify(mongoRepository).findByUserIdIn(userIds);
        verify(domainMapper, times(2)).toDomainWithLimit(any(UserRecommendationsDocument.class), eq(limit));
    }
    
    // Helper method to create a UserRecommendationsDocument with a specific userId
    private UserRecommendationsDocument createUserRecommendationsDocument(int userId) {
        UserRecommendationsDocument document = mock(UserRecommendationsDocument.class);
        when(document.getUserId()).thenReturn(userId);
        return document;
    }
}