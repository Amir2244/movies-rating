package org.hiast.recommendationsapi.adapter.out.messaging.kafka;

import org.apache.fury.Fury;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.hiast.model.MovieRecommendation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * Service for consuming recommendations from Kafka.
 * This service is used to consume recommendations generated by the real-time service.
 */
@Service
public class KafkaConsumerService {
    private static final String DEFAULT_KAFKA_BROKER = "kafka:9092";
    private static final String RECOMMENDATIONS_TOPIC = "recommendations";
    private static final int POLL_TIMEOUT_MS = 10000; // 10 seconds
    private static final int MAX_ATTEMPTS = 3; // Maximum number of attempts to poll for recommendations

    private final String kafkaBroker;
    private final Fury fury;
    private final ExecutorService executorService;

    public KafkaConsumerService(@Value("${spring.kafka.bootstrap-servers:#{null}}") String kafkaBroker) {
        this.kafkaBroker = kafkaBroker != null && !kafkaBroker.isEmpty() ? kafkaBroker : DEFAULT_KAFKA_BROKER;
        this.fury = FurySerializationUtils.createConfiguredFury();
        this.executorService = Executors.newCachedThreadPool();
    }

    /**
     * Asynchronously consumes recommendations for a specific user from Kafka.
     * 
     * @param userId The user ID to get recommendations for
     * @return A CompletableFuture that will be completed with the list of recommendations or null if none are found
     */
    public CompletableFuture<List<MovieRecommendation>> consumeRecommendationsAsync(long userId) {
        return CompletableFuture.supplyAsync(() -> {
            try {
                return consumeRecommendations(userId);
            } catch (Exception e) {
                // Log the exception but don't propagate it
                System.err.println("Failed to consume recommendations: " + e.getMessage());
                e.printStackTrace();
                return null;
            }
        }, executorService);
    }

    /**
     * Consumes recommendations for a specific user from Kafka.
     * 
     * @param userId The user ID to get recommendations for
     * @return The list of recommendations or null if none are found
     */
    private List<MovieRecommendation> consumeRecommendations(long userId) {
        Properties consumerProps = getProperties();

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(RECOMMENDATIONS_TOPIC));

            int attempts = 0;
            while (attempts < MAX_ATTEMPTS) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));

                if (records.isEmpty()) {
                    attempts++;
                    continue;
                }

                for (ConsumerRecord<String, byte[]> record : records) {
                    try {
                        // Check if this recommendation is for our user
                        if (record.key() != null && record.key().equals(String.valueOf(userId))) {
                            // Deserialize the recommendations
                            @SuppressWarnings("unchecked")
                            List<MovieRecommendation> recommendations = 
                                (List<MovieRecommendation>) fury.deserialize(record.value());
                            return recommendations;
                        }
                    } catch (Exception e) {
                        System.err.println("Failed to deserialize recommendations: " + e.getMessage());
                    }
                }

                attempts++;
            }
        }

        return null;
    }

    private Properties getProperties() {
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaBroker);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "recommendations-reader-group");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumerProps.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 10);
        return consumerProps;
    }

    /**
     * Shuts down the executor service.
     */
    public void shutdown() {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(5, TimeUnit.SECONDS)) {
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            executorService.shutdownNow();
            Thread.currentThread().interrupt();
        }
    }
}
