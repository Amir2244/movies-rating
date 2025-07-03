package org.hiast.realtime.application.service;

import org.hiast.events.EventType;
import org.hiast.ids.MovieId;
import org.hiast.ids.UserId;
import org.hiast.model.InteractionEvent;
import org.hiast.model.InteractionEventDetails;
import org.hiast.model.MovieRecommendation;
import org.hiast.model.RatingValue;
import org.hiast.model.factors.UserFactor;
import org.hiast.realtime.application.port.out.RecommendationNotifierPort;
import org.hiast.realtime.application.port.out.UserFactorPort;
import org.hiast.realtime.application.port.out.VectorSearchPort;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.mockito.Mockito.*;

/**
 * Tests for the RealTimeRecommendationService class.
 */
@ExtendWith(MockitoExtension.class)
public class RealTimeRecommendationServiceTest {

    @Mock
    private UserFactorPort userFactorPort;

    @Mock
    private VectorSearchPort vectorSearchPort;

    @Mock
    private RecommendationNotifierPort recommendationNotifierPort;

    private RealTimeRecommendationService service;

    @BeforeEach
    void setUp() {
        service = new RealTimeRecommendationService(userFactorPort, vectorSearchPort, recommendationNotifierPort);
    }

    @Test
    @DisplayName("Should process event and generate recommendations")
    void shouldProcessEventAndGenerateRecommendations() {
        // Arrange
        UserId userId = UserId.of(123);
        MovieId movieId = MovieId.of(456);
        InteractionEventDetails details = new InteractionEventDetails.Builder(userId, EventType.RATING_GIVEN, Instant.now())
                .withMovieId(movieId)
                .withRatingValue(5.0)
                .build();
        InteractionEvent event = new InteractionEvent(userId, movieId, details);

        UserFactor<float[]> userFactor = new UserFactor<>(userId, new float[]{0.1f, 0.2f, 0.3f});
        Instant now = Instant.now();
        List<MovieRecommendation> recommendations = Arrays.asList(
                new MovieRecommendation(userId, MovieId.of(1), 0.9f, now),
                new MovieRecommendation(userId, MovieId.of(2), 0.8f, now),
                new MovieRecommendation(userId, MovieId.of(3), 0.7f, now)
        );

        // Mock the dependencies
        when(userFactorPort.findUserFactorById(userId)).thenReturn(Optional.of(userFactor));
        when(vectorSearchPort.findSimilarItemsByMovie(eq(userFactor), eq(movieId), anyInt(), eq(5.0)))
                .thenReturn(recommendations);

        // Act
        service.processEvent(event);

        // Assert
        verify(userFactorPort).findUserFactorById(userId);
        verify(vectorSearchPort).findSimilarItemsByMovie(eq(userFactor), eq(movieId), anyInt(), eq(5.0));
        verify(recommendationNotifierPort).notify(userId, recommendations);
        assert event.isProcessed();
    }

    @Test
    @DisplayName("Should handle null event")
    void shouldHandleNullEvent() {
        // Act
        service.processEvent(null);

        // Assert
        verifyNoInteractions(userFactorPort, vectorSearchPort, recommendationNotifierPort);
    }

    @Test
    @DisplayName("Should handle event with null user ID")
    void shouldHandleEventWithNullUserId() {
        // Arrange
        MovieId movieId = MovieId.of(456);
        InteractionEventDetails details = new InteractionEventDetails.Builder(null, EventType.MOVIE_VIEWED, Instant.now())
                .withMovieId(movieId)
                .withRatingValue(5.0)
                .build();
        InteractionEvent event = new InteractionEvent(null, movieId, details);

        // Act
        service.processEvent(event);

        // Assert
        verifyNoInteractions(userFactorPort, vectorSearchPort, recommendationNotifierPort);
    }

    @Test
    @DisplayName("Should handle event with null details")
    void shouldHandleEventWithNullDetails() {
        // Arrange
        UserId userId = UserId.of(123);
        MovieId movieId = MovieId.of(456);
        InteractionEvent event = new InteractionEvent(userId, movieId, null);

        // Act
        service.processEvent(event);

        // Assert
        verifyNoInteractions(userFactorPort, vectorSearchPort, recommendationNotifierPort);
    }

    @Test
    @DisplayName("Should handle event with zero weight")
    void shouldHandleEventWithZeroWeight() {
        // Arrange
        UserId userId = UserId.of(123);
        MovieId movieId = MovieId.of(456);
        InteractionEventDetails details = new InteractionEventDetails.Builder(userId, EventType.UNKNOWN, Instant.now())
                .withMovieId(movieId)
                .withRatingValue(0.0)
                .build();
        InteractionEvent event = new InteractionEvent(userId, movieId, details);

        // Act
        service.processEvent(event);

        // Assert
        verifyNoInteractions(userFactorPort, vectorSearchPort, recommendationNotifierPort);
    }

    @Test
    @DisplayName("Should handle event with negative weight")
    void shouldHandleEventWithNegativeWeight() {
        // Arrange
        UserId userId = UserId.of(123);
        MovieId movieId = MovieId.of(456);
        InteractionEventDetails details = new InteractionEventDetails.Builder(userId, EventType.UNKNOWN, Instant.now())
                .withMovieId(movieId)
                .withRatingValue(-1.0)
                .build();
        InteractionEvent event = new InteractionEvent(userId, movieId, details);

        // Act
        service.processEvent(event);

        // Assert
        verifyNoInteractions(userFactorPort, vectorSearchPort, recommendationNotifierPort);
    }

    @Test
    @DisplayName("Should handle missing user factor")
    void shouldHandleMissingUserFactor() {
        // Arrange
        UserId userId = UserId.of(123);
        MovieId movieId = MovieId.of(456);
        InteractionEventDetails details = new InteractionEventDetails.Builder(userId, EventType.RATING_GIVEN, Instant.now())
                .withMovieId(movieId)
                .withRatingValue(5.0)
                .build();
        InteractionEvent event = new InteractionEvent(userId, movieId, details);

        // Mock the dependencies
        when(userFactorPort.findUserFactorById(userId)).thenReturn(Optional.empty());

        // Act
        service.processEvent(event);

        // Assert
        verify(userFactorPort).findUserFactorById(userId);
        verifyNoInteractions(vectorSearchPort, recommendationNotifierPort);
    }

    @Test
    @DisplayName("Should handle null movie ID")
    void shouldHandleNullMovieId() {
        // Arrange
        UserId userId = UserId.of(123);
        InteractionEventDetails details = new InteractionEventDetails.Builder(userId, EventType.RATING_GIVEN, Instant.now())
                .withRatingValue(5.0)
                .build();
        InteractionEvent event = new InteractionEvent(userId, null, details);

        UserFactor<float[]> userFactor = new UserFactor<>(userId, new float[]{0.1f, 0.2f, 0.3f});

        // Mock the dependencies
        when(userFactorPort.findUserFactorById(userId)).thenReturn(Optional.of(userFactor));

        // Act
        service.processEvent(event);

        // Assert
        verify(userFactorPort).findUserFactorById(userId);
        verifyNoInteractions(vectorSearchPort, recommendationNotifierPort);
    }

    @Test
    @DisplayName("Should handle empty recommendations")
    void shouldHandleEmptyRecommendations() {
        // Arrange
        UserId userId = UserId.of(123);
        MovieId movieId = MovieId.of(456);
        InteractionEventDetails details = new InteractionEventDetails.Builder(userId, EventType.RATING_GIVEN, Instant.now())
                .withMovieId(movieId)
                .withRatingValue(5.0)
                .build();
        InteractionEvent event = new InteractionEvent(userId, movieId, details);

        UserFactor<float[]> userFactor = new UserFactor<>(userId, new float[]{0.1f, 0.2f, 0.3f});
        List<MovieRecommendation> recommendations = new ArrayList<>();

        // Mock the dependencies
        when(userFactorPort.findUserFactorById(userId)).thenReturn(Optional.of(userFactor));
        when(vectorSearchPort.findSimilarItemsByMovie(eq(userFactor), eq(movieId), anyInt(), eq(5.0)))
                .thenReturn(recommendations);

        // Act
        service.processEvent(event);

        // Assert
        verify(userFactorPort).findUserFactorById(userId);
        verify(vectorSearchPort).findSimilarItemsByMovie(eq(userFactor), eq(movieId), anyInt(), eq(5.0));
        verifyNoInteractions(recommendationNotifierPort);
        assert event.isProcessed();
    }
}
