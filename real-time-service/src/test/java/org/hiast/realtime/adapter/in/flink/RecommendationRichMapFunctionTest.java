package org.hiast.realtime.adapter.in.flink;

import org.apache.flink.configuration.Configuration;
import org.hiast.events.EventType;
import org.hiast.ids.MovieId;
import org.hiast.ids.UserId;
import org.hiast.model.InteractionEvent;
import org.hiast.model.InteractionEventDetails;
import org.hiast.realtime.adapter.out.kafka.KafkaNotifierAdapter;
import org.hiast.realtime.adapter.out.redis.RedisUserFactorAdapter;
import org.hiast.realtime.adapter.out.redis.RedisVectorSearchAdapter;
import org.hiast.realtime.application.port.in.ProcessInteractionEventUseCase;
import org.hiast.realtime.application.service.RealTimeRecommendationService;
import org.hiast.realtime.config.AppConfig;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.MockedConstruction;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import redis.clients.jedis.UnifiedJedis;

import java.time.Instant;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Tests for the RecommendationRichMapFunction class.
 */
@ExtendWith(MockitoExtension.class)
public class RecommendationRichMapFunctionTest {

    @Mock
    private AppConfig mockAppConfig;

    @Mock
    private UnifiedJedis mockJedis;

    @Mock
    private KafkaNotifierAdapter mockKafkaNotifierAdapter;

    @Mock
    private ProcessInteractionEventUseCase mockProcessInteractionEventUseCase;

    private RecommendationRichMapFunction function;
    private InteractionEvent testEvent;

    @BeforeEach
    void setUp() {
        function = new RecommendationRichMapFunction();
        
        // Create a test event
        UserId userId = UserId.of(123);
        MovieId movieId = MovieId.of(456);
        Instant now = Instant.now();
        InteractionEventDetails details = new InteractionEventDetails.Builder(userId, EventType.MOVIE_VIEWED, now)
                .withMovieId(movieId)
                .build();
        testEvent = new InteractionEvent(userId, movieId, details);
    }

    @Test
    @DisplayName("Should initialize dependencies in open method")
    void shouldInitializeDependenciesInOpenMethod() throws Exception {
        // Arrange
        try (MockedStatic<AppConfig> mockedAppConfig = mockStatic(AppConfig.class)) {
            mockedAppConfig.when(() -> new AppConfig("real-time-config.properties")).thenReturn(mockAppConfig);
            when(mockAppConfig.getUnifiedJedis()).thenReturn(mockJedis);
            
            try (MockedConstruction<RedisUserFactorAdapter> mockedUserFactorAdapter = mockConstruction(RedisUserFactorAdapter.class);
                 MockedConstruction<RedisVectorSearchAdapter> mockedVectorSearchAdapter = mockConstruction(RedisVectorSearchAdapter.class);
                 MockedConstruction<KafkaNotifierAdapter> mockedKafkaNotifierAdapter = mockConstruction(KafkaNotifierAdapter.class, 
                     (mock, context) -> {
                         assertEquals(1, context.getCount());
                         assertEquals(mockAppConfig, context.arguments().get(0));
                     });
                 MockedConstruction<RealTimeRecommendationService> mockedService = mockConstruction(RealTimeRecommendationService.class)) {
                
                // Act
                function.open(new Configuration());
                
                // Assert
                assertEquals(1, mockedUserFactorAdapter.constructed().size());
                assertEquals(1, mockedVectorSearchAdapter.constructed().size());
                assertEquals(1, mockedKafkaNotifierAdapter.constructed().size());
                assertEquals(1, mockedService.constructed().size());
            }
        }
    }

    @Test
    @DisplayName("Should close connections in close method")
    void shouldCloseConnectionsInCloseMethod() throws Exception {
        // Arrange
        try (MockedStatic<AppConfig> mockedAppConfig = mockStatic(AppConfig.class)) {
            mockedAppConfig.when(() -> new AppConfig("real-time-config.properties")).thenReturn(mockAppConfig);
            when(mockAppConfig.getUnifiedJedis()).thenReturn(mockJedis);
            
            try (MockedConstruction<RedisUserFactorAdapter> mockedUserFactorAdapter = mockConstruction(RedisUserFactorAdapter.class);
                 MockedConstruction<RedisVectorSearchAdapter> mockedVectorSearchAdapter = mockConstruction(RedisVectorSearchAdapter.class);
                 MockedConstruction<KafkaNotifierAdapter> mockedKafkaNotifierAdapter = mockConstruction(KafkaNotifierAdapter.class);
                 MockedConstruction<RealTimeRecommendationService> mockedService = mockConstruction(RealTimeRecommendationService.class)) {
                
                function.open(new Configuration());
                
                // Act
                function.close();
                
                // Assert
                verify(mockJedis).close();
                verify(mockedKafkaNotifierAdapter.constructed().get(0)).close();
            }
        }
    }

    @Test
    @DisplayName("Should process event in map method")
    void shouldProcessEventInMapMethod() throws Exception {
        // Arrange - Use reflection to set the private field
        java.lang.reflect.Field field = RecommendationRichMapFunction.class.getDeclaredField("processInteractionEventUseCase");
        field.setAccessible(true);
        field.set(function, mockProcessInteractionEventUseCase);
        
        // Act
        InteractionEvent result = function.map(testEvent);
        
        // Assert
        verify(mockProcessInteractionEventUseCase).processEvent(testEvent);
        assertSame(testEvent, result);
    }

    @Test
    @DisplayName("Should handle null event in map method")
    void shouldHandleNullEventInMapMethod() throws Exception {
        // Arrange - Use reflection to set the private field
        java.lang.reflect.Field field = RecommendationRichMapFunction.class.getDeclaredField("processInteractionEventUseCase");
        field.setAccessible(true);
        field.set(function, mockProcessInteractionEventUseCase);
        
        // Act
        InteractionEvent result = function.map(null);
        
        // Assert
        verify(mockProcessInteractionEventUseCase).processEvent(null);
        assertNull(result);
    }

    @Test
    @DisplayName("Should handle exception in map method")
    void shouldHandleExceptionInMapMethod() throws Exception {
        // Arrange - Use reflection to set the private field
        java.lang.reflect.Field field = RecommendationRichMapFunction.class.getDeclaredField("processInteractionEventUseCase");
        field.setAccessible(true);
        field.set(function, mockProcessInteractionEventUseCase);
        
        doThrow(new RuntimeException("Processing failed")).when(mockProcessInteractionEventUseCase).processEvent(testEvent);
        
        // Act & Assert
        assertThrows(RuntimeException.class, () -> function.map(testEvent));
    }
}