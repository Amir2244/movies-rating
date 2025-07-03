package org.hiast.recommendationsapi.config;

import org.hiast.model.MovieRecommendation;
import org.hiast.recommendationsapi.adapter.out.messaging.kafka.KafkaConsumerService;
import org.springframework.boot.test.context.TestConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Primary;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Test configuration that provides mock beans for testing.
 * This configuration is only active during tests.
 */
@TestConfiguration
public class TestConfig {

    /**
     * Provides a mock KafkaConsumerService that doesn't connect to Kafka.
     * This bean will replace the real KafkaConsumerService during tests.
     */
    @Bean
    @Primary
    public KafkaConsumerService kafkaConsumerService() {
        return new MockKafkaConsumerService();
    }

    /**
     * Mock implementation of KafkaConsumerService that doesn't connect to Kafka.
     */
    public static class MockKafkaConsumerService extends KafkaConsumerService {
        private final ExecutorService executorService;

        public MockKafkaConsumerService() {
            // Pass empty string to avoid connecting to a real Kafka broker
            super("");
            this.executorService = Executors.newSingleThreadExecutor();
        }

        @Override
        public CompletableFuture<List<MovieRecommendation>> consumeRecommendationsAsync(long userId) {
            // Return an empty list immediately without connecting to Kafka
            return CompletableFuture.completedFuture(List.of());
        }

        @Override
        public void shutdown() {
            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(1, TimeUnit.SECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }
}
