package org.hiast;

import org.apache.fury.Fury;
import org.apache.fury.config.Language;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.hiast.events.EventType;
import org.hiast.ids.MovieId;
import org.hiast.ids.UserId;
import org.hiast.model.InteractionEventDetails;
import org.hiast.model.MovieRecommendation;
import org.hiast.realtime.domain.model.InteractionEvent;

import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Properties;

/**
 * A utility to produce a Fury-serialized InteractionEvent to Kafka for testing
 * and receive back both the processed event and the generated recommendations.
 * 
 * This class:
 * 1. Creates and sends a sample InteractionEvent to the input topic
 * 2. Listens for the processed event on the output topic
 * 3. Displays the details of the processed event when received
 * 4. Listens for recommendations on the recommendations topic
 * 5. Displays the movie recommendations with their predicted ratings
 */
public class KafkaEventProducer {

    private static final String KAFKA_BROKER = "localhost:9094";
    private static final String INPUT_TOPIC = "user_interactions";
    private static final String OUTPUT_TOPIC = "processed-events";
    private static final String RECOMMENDATIONS_TOPIC = "recommendations";
    private static final int POLL_TIMEOUT_MS = 10000; // 10 seconds

    public static void main(String[] args) {
        // 1. Initialize Fury, configured exactly like in the Flink DeserializationSchema
        Fury fury = Fury.builder()
                .withLanguage(Language.JAVA)
                .requireClassRegistration(false)
                .withRefTracking(true)
                .build();

        // 2. Create a sample InteractionEvent object
        UserId userId = UserId.of(23);
        MovieId movieId = MovieId.of(177209);
        InteractionEventDetails details = new InteractionEventDetails.Builder(userId, EventType.ADDED_TO_WATCHLIST, Instant.now())
                .withRatingValue(5)
                .withMovieId(movieId)
                .build();

        InteractionEvent event = new InteractionEvent(userId, movieId, details);

        // 3. Set up Kafka Producer properties
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        // 4. Serialize the event object into a byte array using Fury
        byte[] serializedEvent = fury.serialize(event);
        System.out.println("Serialized event to " + serializedEvent.length + " bytes.");

        // 5. Send the serialized byte array to the Kafka topic
        try (Producer<String, byte[]> producer = new KafkaProducer<>(producerProps)) {
            ProducerRecord<String, byte[]> record = new ProducerRecord<>(INPUT_TOPIC, null, serializedEvent);
            producer.send(record, (metadata, exception) -> {
                if (exception == null) {
                    System.out.printf("Successfully sent message to topic %s, partition %d, offset %d%n",
                            metadata.topic(), metadata.partition(), metadata.offset());
                } else {
                    System.err.println("Failed to send message");
                    exception.printStackTrace();
                }
            });
            producer.flush();
            System.out.println("Test event sent to Kafka topic: " + INPUT_TOPIC);
        }

        // 6. Set up Kafka Consumer properties
        Properties consumerProps = new Properties();
        consumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        consumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "event-producer-consumer");
        consumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        // 7. Create a consumer and subscribe to the processed events topic
        System.out.println("Waiting for processed events on topic: " + OUTPUT_TOPIC);
        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(consumerProps)) {
            consumer.subscribe(Collections.singletonList(OUTPUT_TOPIC));

            // 8. Poll for processed events
            boolean received = false;
            int attempts = 0;
            while (!received && attempts < 5) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));

                if (records.isEmpty()) {
                    System.out.println("No records received yet. Waiting... (Attempt " + (++attempts) + "/5)");
                } else {
                    for (ConsumerRecord<String, byte[]> record : records) {
                        try {
                            // 9. Deserialize the processed event
                            InteractionEvent processedEvent = (InteractionEvent) fury.deserialize(record.value());
                            System.out.println("\nReceived processed event:");
                            System.out.println("  User ID: " + processedEvent.getUserId().getUserId());
                            System.out.println("  Movie ID: " + processedEvent.getMovieId().getMovieId());
                            System.out.println("  Event Type: " + processedEvent.getDetails().getEventType());
                            System.out.println("  Processed: " + processedEvent.isProcessed());
                            System.out.println("  Timestamp: " + processedEvent.getDetails().getTimestamp());
                            received = true;
                        } catch (Exception e) {
                            System.err.println("Failed to deserialize processed event");
                            e.printStackTrace();
                        }
                    }
                }
            }

            if (!received) {
                System.out.println("No processed events received after 5 attempts. Please check if the processing service is running.");
            } else {
                // After receiving the processed event, listen for recommendations
                System.out.println("\nWaiting for recommendations on topic: " + RECOMMENDATIONS_TOPIC);
                listenForRecommendations(fury, userId.getUserId());
            }
        }
    }

    /**
     * Listens for recommendations on the recommendations topic.
     * 
     * @param fury The Fury instance for deserialization
     * @param userId The user ID to filter recommendations for
     */
    private static void listenForRecommendations(Fury fury, long userId) {
        // Set up Kafka Consumer properties for recommendations
        Properties recommendationsConsumerProps = new Properties();
        recommendationsConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        recommendationsConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "recommendations-consumer");
        recommendationsConsumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        recommendationsConsumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        recommendationsConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(recommendationsConsumerProps)) {
            consumer.subscribe(Collections.singletonList(RECOMMENDATIONS_TOPIC));

            boolean received = false;
            int attempts = 0;
            while (!received && attempts < 5) {
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));

                if (records.isEmpty()) {
                    System.out.println("No recommendations received yet. Waiting... (Attempt " + (++attempts) + "/5)");
                } else {
                    for (ConsumerRecord<String, byte[]> record : records) {
                        // Check if this recommendation is for our user
                        if (record.key() != null && record.key().equals(String.valueOf(userId))) {
                            try {
                                // Deserialize the recommendations
                                @SuppressWarnings("unchecked")
                                List<MovieRecommendation> recommendations = (List<MovieRecommendation>) fury.deserialize(record.value());

                                System.out.println("\nReceived recommendations for user " + userId + ":");
                                System.out.println("--------------------------------------------");

                                if (recommendations.isEmpty()) {
                                    System.out.println("No recommendations available.");
                                } else {
                                    for (int i = 0; i < recommendations.size(); i++) {
                                        MovieRecommendation rec = recommendations.get(i);
                                        System.out.printf("%d. Movie ID: %d, Rating: %.2f%n", 
                                                i + 1, rec.getMovieId().getMovieId(), rec.getPredictedRating());
                                    }
                                }

                                received = true;
                                break;
                            } catch (Exception e) {
                                System.err.println("Failed to deserialize recommendations");
                                e.printStackTrace();
                            }
                        }
                    }
                }
            }

            if (!received) {
                System.out.println("No recommendations received after 5 attempts. The system might not be generating recommendations for this user or event type.");
            }
        }
    }
}
