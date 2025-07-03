package org.hiast.realtime.adapter.in.kafka;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.fury.Fury;
import org.hiast.ids.MovieId;
import org.hiast.ids.UserId;
import org.hiast.model.InteractionEvent;
import org.hiast.model.InteractionEventDetails;
import org.hiast.events.EventType;
import org.hiast.realtime.util.FurySerializationUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;

import java.io.IOException;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for the FuryDeserializationSchema class.
 */
@ExtendWith(MockitoExtension.class)
public class FuryDeserializationSchemaTest {

    @Mock
    private Fury mockFury;

    private FuryDeserializationSchema schema;
    private InteractionEvent testEvent;
    private byte[] serializedEvent;

    @BeforeEach
    void setUp() {
        schema = new FuryDeserializationSchema();

        // Create a test event
        UserId userId = UserId.of(123);
        MovieId movieId = MovieId.of(456);
        Instant now = Instant.now();
        InteractionEventDetails details = new InteractionEventDetails.Builder(userId, EventType.MOVIE_VIEWED, now)
                .withMovieId(movieId)
                .build();
        testEvent = new InteractionEvent(userId, movieId, details);

        // Create a real Fury instance for serialization
        Fury realFury = FurySerializationUtils.createConfiguredFury();
        serializedEvent = realFury.serialize(testEvent);
    }

    @Test
    @DisplayName("Should initialize Fury in open method")
    void shouldInitializeFuryInOpenMethod() {
        // Arrange
        try (MockedStatic<FurySerializationUtils> mockedUtils = mockStatic(FurySerializationUtils.class)) {
            mockedUtils.when(FurySerializationUtils::createConfiguredFury).thenReturn(mockFury);

            // Act
            schema.open(mock(DeserializationSchema.InitializationContext.class));

            // Assert
            mockedUtils.verify(FurySerializationUtils::createConfiguredFury);
        }
    }

    @Test
    @DisplayName("Should deserialize valid message")
    void shouldDeserializeValidMessage() throws IOException {
        // Arrange
        schema.open(mock(DeserializationSchema.InitializationContext.class));

        // Act
        InteractionEvent result = schema.deserialize(serializedEvent);

        // Assert
        assertNotNull(result);
        assertEquals(testEvent.getUserId().getUserId(), result.getUserId().getUserId());
        assertEquals(testEvent.getMovieId().getMovieId(), result.getMovieId().getMovieId());
        assertEquals(testEvent.getDetails().getEventType(), result.getDetails().getEventType());
    }

    @Test
    @DisplayName("Should return null for null message")
    void shouldReturnNullForNullMessage() throws IOException {
        // Arrange
        schema.open(mock(DeserializationSchema.InitializationContext.class));

        // Act
        InteractionEvent result = schema.deserialize(null);

        // Assert
        assertNull(result);
    }

    @Test
    @DisplayName("Should return null for empty message")
    void shouldReturnNullForEmptyMessage() throws IOException {
        // Arrange
        schema.open(mock(DeserializationSchema.InitializationContext.class));

        // Act
        InteractionEvent result = schema.deserialize(new byte[0]);

        // Assert
        assertNull(result);
    }

    @Test
    @DisplayName("Should retry deserialization on failure")
    void shouldRetryDeserializationOnFailure() throws IOException {
        // Arrange
        try (MockedStatic<FurySerializationUtils> mockedUtils = mockStatic(FurySerializationUtils.class)) {
            mockedUtils.when(FurySerializationUtils::createConfiguredFury).thenReturn(mockFury);

            // First attempt fails, second succeeds
            when(mockFury.deserialize(any(byte[].class)))
                .thenThrow(new RuntimeException("Deserialization failed"))
                .thenReturn(testEvent);

            schema.open(mock(DeserializationSchema.InitializationContext.class));

            // Act
            InteractionEvent result = schema.deserialize(serializedEvent);

            // Assert
            assertNotNull(result);
            verify(mockFury, times(2)).deserialize(any(byte[].class));
            mockedUtils.verify(FurySerializationUtils::createConfiguredFury, times(2));
        }
    }

    @Test
    @DisplayName("Should return null when both deserialization attempts fail")
    void shouldReturnNullWhenBothDeserializationAttemptsFail() throws IOException {
        // Arrange
        try (MockedStatic<FurySerializationUtils> mockedUtils = mockStatic(FurySerializationUtils.class)) {
            mockedUtils.when(FurySerializationUtils::createConfiguredFury).thenReturn(mockFury);

            // Both attempts fail
            when(mockFury.deserialize(any(byte[].class)))
                .thenThrow(new RuntimeException("First failure"))
                .thenThrow(new RuntimeException("Second failure"));

            schema.open(mock(DeserializationSchema.InitializationContext.class));

            // Act
            InteractionEvent result = schema.deserialize(serializedEvent);

            // Assert
            assertNull(result);
            verify(mockFury, times(2)).deserialize(any(byte[].class));
            mockedUtils.verify(FurySerializationUtils::createConfiguredFury, times(2));
        }
    }

    @Test
    @DisplayName("Should always return false for isEndOfStream")
    void shouldAlwaysReturnFalseForIsEndOfStream() {
        // Act
        boolean result = schema.isEndOfStream(testEvent);

        // Assert
        assertFalse(result);
    }

    @Test
    @DisplayName("Should return correct TypeInformation")
    void shouldReturnCorrectTypeInformation() {
        // Act
        TypeInformation<InteractionEvent> typeInfo = schema.getProducedType();

        // Assert
        assertNotNull(typeInfo);
        assertEquals(InteractionEvent.class, typeInfo.getTypeClass());
    }
}
