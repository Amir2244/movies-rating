package org.hiast.realtime.adapter.out.kafka;

import org.apache.flink.api.common.serialization.SerializationSchema;
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

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for the FurySerializationSchema class.
 */
@ExtendWith(MockitoExtension.class)
public class FurySerializationSchemaTest {

    @Mock
    private Fury mockFury;

    private FurySerializationSchema schema;
    private InteractionEvent testEvent;
    private byte[] expectedSerializedData;

    @BeforeEach
    void setUp() {
        schema = new FurySerializationSchema();
        
        // Create a test event
        UserId userId = UserId.of(123);
        MovieId movieId = MovieId.of(456);
        Instant now = Instant.now();
        InteractionEventDetails details = new InteractionEventDetails.Builder(userId, EventType.MOVIE_VIEWED, now)
                .withMovieId(movieId)
                .build();
        testEvent = new InteractionEvent(userId, movieId, details);
        
        // Create expected serialized data
        Fury realFury = FurySerializationUtils.createConfiguredFury();
        expectedSerializedData = realFury.serialize(testEvent);
    }

    @Test
    @DisplayName("Should initialize Fury in open method")
    void shouldInitializeFuryInOpenMethod() {
        // Arrange
        try (MockedStatic<FurySerializationUtils> mockedUtils = mockStatic(FurySerializationUtils.class)) {
            mockedUtils.when(FurySerializationUtils::createConfiguredFury).thenReturn(mockFury);
            
            // Act
            schema.open(mock(SerializationSchema.InitializationContext.class));
            
            // Assert
            mockedUtils.verify(FurySerializationUtils::createConfiguredFury);
        }
    }

    @Test
    @DisplayName("Should serialize valid event")
    void shouldSerializeValidEvent() {
        // Arrange
        try (MockedStatic<FurySerializationUtils> mockedUtils = mockStatic(FurySerializationUtils.class)) {
            mockedUtils.when(FurySerializationUtils::createConfiguredFury).thenReturn(mockFury);
            when(mockFury.serialize(testEvent)).thenReturn(expectedSerializedData);
            
            schema.open(mock(SerializationSchema.InitializationContext.class));
            
            // Act
            byte[] result = schema.serialize(testEvent);
            
            // Assert
            assertNotNull(result);
            assertArrayEquals(expectedSerializedData, result);
            verify(mockFury).serialize(testEvent);
        }
    }

    @Test
    @DisplayName("Should return empty byte array for null event")
    void shouldReturnEmptyByteArrayForNullEvent() {
        // Arrange
        schema.open(mock(SerializationSchema.InitializationContext.class));
        
        // Act
        byte[] result = schema.serialize(null);
        
        // Assert
        assertNotNull(result);
        assertEquals(0, result.length);
    }

    @Test
    @DisplayName("Should handle serialization exception")
    void shouldHandleSerializationException() {
        // Arrange
        try (MockedStatic<FurySerializationUtils> mockedUtils = mockStatic(FurySerializationUtils.class)) {
            mockedUtils.when(FurySerializationUtils::createConfiguredFury).thenReturn(mockFury);
            when(mockFury.serialize(testEvent)).thenThrow(new RuntimeException("Serialization failed"));
            
            schema.open(mock(SerializationSchema.InitializationContext.class));
            
            // Act
            byte[] result = schema.serialize(testEvent);
            
            // Assert
            assertNotNull(result);
            assertEquals(0, result.length);
            verify(mockFury).serialize(testEvent);
        }
    }

    @Test
    @DisplayName("Should serialize event with real Fury instance")
    void shouldSerializeEventWithRealFuryInstance() {
        // Arrange
        schema.open(mock(SerializationSchema.InitializationContext.class));
        
        // Act
        byte[] result = schema.serialize(testEvent);
        
        // Assert
        assertNotNull(result);
        assertTrue(result.length > 0);
        
        // Verify we can deserialize the result back to an event
        Fury realFury = FurySerializationUtils.createConfiguredFury();
        InteractionEvent deserializedEvent = (InteractionEvent) realFury.deserialize(result);
        assertNotNull(deserializedEvent);
        assertEquals(testEvent.getUserId().getUserId(), deserializedEvent.getUserId().getUserId());
        assertEquals(testEvent.getMovieId().getMovieId(), deserializedEvent.getMovieId().getMovieId());
    }
}