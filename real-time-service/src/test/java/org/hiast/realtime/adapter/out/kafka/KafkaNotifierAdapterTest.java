package org.hiast.realtime.adapter.out.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.hiast.ids.MovieId;
import org.hiast.ids.UserId;
import org.hiast.model.MovieRecommendation;
import org.hiast.realtime.config.AppConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for the KafkaNotifierAdapter class.
 */
@ExtendWith(MockitoExtension.class)
public class KafkaNotifierAdapterTest {

    @Mock
    private AppConfig mockAppConfig;

    @Mock
    private KafkaProducer<String, List<MovieRecommendation>> mockProducer;

    private KafkaNotifierAdapter adapter;
    private UserId testUserId;
    private List<MovieRecommendation> testRecommendations;

    @BeforeEach
    void setUp() {
        // Mock the AppConfig
        when(mockAppConfig.getProperty("kafka.bootstrap.servers")).thenReturn("localhost:9092");
        when(mockAppConfig.getProperty("kafka.topic.output")).thenReturn("test-recommendations");

        // Create test data
        testUserId = UserId.of(123);
        Instant now = Instant.now();
        testRecommendations = List.of(
                new MovieRecommendation(testUserId, MovieId.of(456), 0.9f, now),
                new MovieRecommendation(testUserId, MovieId.of(789), 0.8f, now)
        );
    }

    @Test
    @DisplayName("Should initialize with correct configuration")
    void shouldInitializeWithCorrectConfiguration() {
        // Arrange & Act
        try (MockedConstruction<KafkaProducer> mockedProducer = 
                mockConstruction(KafkaProducer.class, (mock, context) -> {
                    // Verify the properties passed to the KafkaProducer constructor
                    assertTrue(context.arguments().get(0) instanceof Properties);
                    Properties props = (Properties) context.arguments().get(0);
                    assertEquals("localhost:9092", props.get("bootstrap.servers"));
                    assertEquals("org.apache.kafka.common.serialization.StringSerializer", props.get("key.serializer"));
                    assertEquals("org.hiast.realtime.adapter.out.kafka.RecommendationListSerializer", props.get("value.serializer"));
                })) {

            // Create the adapter
            adapter = new KafkaNotifierAdapter(mockAppConfig);

            // Assert
            assertEquals(1, mockedProducer.constructed().size());
        }
    }

    @Test
    @DisplayName("Should use default topic when not specified in config")
    void shouldUseDefaultTopicWhenNotSpecifiedInConfig() {
        // Arrange
        when(mockAppConfig.getProperty("kafka.topic.output")).thenReturn(null);

        // Act
        try (MockedConstruction<KafkaProducer> mockedProducer = 
                mockConstruction(KafkaProducer.class)) {

            // Create the adapter
            adapter = new KafkaNotifierAdapter(mockAppConfig);

            // Use reflection to get the outputTopic field
            java.lang.reflect.Field field;
            try {
                field = KafkaNotifierAdapter.class.getDeclaredField("outputTopic");
                field.setAccessible(true);
                String outputTopic = (String) field.get(adapter);

                // Assert
                assertEquals("recommendations", outputTopic);
            } catch (Exception e) {
                fail("Failed to access outputTopic field: " + e.getMessage());
            }
        }
    }

    @Test
    @DisplayName("Should send recommendations to Kafka")
    void shouldSendRecommendationsToKafka() {
        // Arrange
        try (MockedConstruction<KafkaProducer> mockedProducer = 
                mockConstruction(KafkaProducer.class, (mock, context) -> {
                    // Store the mock for later verification
                    KafkaProducer<String, List<MovieRecommendation>> typedMock = 
                            (KafkaProducer<String, List<MovieRecommendation>>) mock;
                    when(typedMock.send(any(), any())).thenReturn(null);
                })) {

            // Create the adapter
            adapter = new KafkaNotifierAdapter(mockAppConfig);

            // Act
            adapter.notify(testUserId, testRecommendations);

            // Assert
            ArgumentCaptor<ProducerRecord<String, List<MovieRecommendation>>> recordCaptor = 
                    ArgumentCaptor.forClass(ProducerRecord.class);
            KafkaProducer<String, List<MovieRecommendation>> typedMock = 
                    (KafkaProducer<String, List<MovieRecommendation>>) mockedProducer.constructed().get(0);
            verify(typedMock).send(recordCaptor.capture(), any());

            ProducerRecord<String, List<MovieRecommendation>> capturedRecord = recordCaptor.getValue();
            assertEquals("test-recommendations", capturedRecord.topic());
            assertEquals("123", capturedRecord.key());
            assertEquals(testRecommendations, capturedRecord.value());
        }
    }

    @Test
    @DisplayName("Should handle null user ID")
    void shouldHandleNullUserId() {
        // Arrange
        try (MockedConstruction<KafkaProducer> mockedProducer = 
                mockConstruction(KafkaProducer.class)) {

            // Create the adapter
            adapter = new KafkaNotifierAdapter(mockAppConfig);

            // Act
            adapter.notify(null, testRecommendations);

            // Assert
            verifyNoInteractions(mockedProducer.constructed().get(0));
        }
    }

    @Test
    @DisplayName("Should handle null recommendations")
    void shouldHandleNullRecommendations() {
        // Arrange
        try (MockedConstruction<KafkaProducer> mockedProducer = 
                mockConstruction(KafkaProducer.class)) {

            // Create the adapter
            adapter = new KafkaNotifierAdapter(mockAppConfig);

            // Act
            adapter.notify(testUserId, null);

            // Assert
            verifyNoInteractions(mockedProducer.constructed().get(0));
        }
    }

    @Test
    @DisplayName("Should handle empty recommendations")
    void shouldHandleEmptyRecommendations() {
        // Arrange
        try (MockedConstruction<KafkaProducer> mockedProducer = 
                mockConstruction(KafkaProducer.class)) {

            // Create the adapter
            adapter = new KafkaNotifierAdapter(mockAppConfig);

            // Act
            adapter.notify(testUserId, new ArrayList<>());

            // Assert
            verifyNoInteractions(mockedProducer.constructed().get(0));
        }
    }

    @Test
    @DisplayName("Should handle exception when sending recommendations")
    void shouldHandleExceptionWhenSendingRecommendations() {
        // Arrange
        try (MockedConstruction<KafkaProducer> mockedProducer = 
                mockConstruction(KafkaProducer.class, (mock, context) -> {
                    KafkaProducer<String, List<MovieRecommendation>> typedMock = 
                            (KafkaProducer<String, List<MovieRecommendation>>) mock;
                    when(typedMock.send(any(), any())).thenThrow(new RuntimeException("Sending failed"));
                })) {

            // Create the adapter
            adapter = new KafkaNotifierAdapter(mockAppConfig);

            // Act & Assert - Should not throw exception
            assertDoesNotThrow(() -> adapter.notify(testUserId, testRecommendations));
        }
    }

    @Test
    @DisplayName("Should close Kafka producer")
    void shouldCloseKafkaProducer() {
        // Arrange
        try (MockedConstruction<KafkaProducer> mockedProducer = 
                mockConstruction(KafkaProducer.class)) {

            // Create the adapter
            adapter = new KafkaNotifierAdapter(mockAppConfig);

            // Act
            adapter.close();

            // Assert
            verify(mockedProducer.constructed().get(0)).close();
        }
    }
}
