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
import org.hiast.realtime.util.FurySerializationUtils;
import org.hiast.events.EventType;
import org.hiast.ids.MovieId;
import org.hiast.ids.UserId;
import org.hiast.model.InteractionEventDetails;
import org.hiast.model.MovieRecommendation;
import org.hiast.model.RatingValue;
import org.hiast.realtime.domain.model.InteractionEvent;

import java.time.Duration;
import java.time.Instant;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicInteger;

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

    // Configuration for random event generation
    private static final int DEFAULT_NUM_EVENTS = 1; // Default number of events to send
    private static final int MIN_USER_ID = 1;
    private static final int MAX_USER_ID = 10000;
    private static final int MIN_MOVIE_ID = 100000;
    private static final int MAX_MOVIE_ID = 200000;
    private static final Random RANDOM = new Random();

    /**
     * Generates a random user ID within the configured range.
     * @return A random UserId
     */
    private static UserId generateRandomUserId() {
        int id = MIN_USER_ID + RANDOM.nextInt(MAX_USER_ID - MIN_USER_ID + 1);
        return UserId.of(id);
    }

    /**
     * Generates a random movie ID within the configured range.
     * @return A random MovieId
     */
    private static MovieId generateRandomMovieId() {
        int id = MIN_MOVIE_ID + RANDOM.nextInt(MAX_MOVIE_ID - MIN_MOVIE_ID + 1);
        return MovieId.of(id);
    }

    /**
     * Selects a random event type from the available options.
     * @return A random EventType
     */
    private static EventType getRandomEventType() {
        EventType[] eventTypes = {
            EventType.RATING_GIVEN,
            EventType.MOVIE_VIEWED,
            EventType.ADDED_TO_WATCHLIST,
            EventType.MOVIE_CLICKED,
            EventType.SEARCHED_FOR_MOVIE
        };
        return eventTypes[RANDOM.nextInt(eventTypes.length)];
    }

    /**
     * Generates a random rating value between 0.5 and 5.0.
     * @return A random RatingValue
     */
    private static RatingValue generateRandomRatingValue() {
        // Generate a random rating between 0.5 and 5.0 with 0.5 increments
        double[] possibleRatings = {0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5, 4.0, 4.5, 5.0};
        return RatingValue.of(possibleRatings[RANDOM.nextInt(possibleRatings.length)]);
    }

    /**
     * Creates a random interaction event.
     * @return A random InteractionEvent
     */
    private static InteractionEvent createRandomEvent() {
        UserId userId = generateRandomUserId();
        MovieId movieId = generateRandomMovieId();
        EventType eventType = getRandomEventType();

        InteractionEventDetails.Builder builder = new InteractionEventDetails.Builder(
                userId, eventType, Instant.now())
                .withMovieId(MovieId.of(195791));

        // Add rating value if the event type is RATING_GIVEN
        if (eventType == EventType.RATING_GIVEN) {
            builder.withRatingValue(generateRandomRatingValue());
        }

        InteractionEventDetails details = builder.build();
        return new InteractionEvent(userId, MovieId.of(195791), details);
    }

    public static void main(String[] args) {
        // Parse command-line arguments for number of events to send
        int parsedEvents = DEFAULT_NUM_EVENTS;
        if (args.length > 0) {
            try {
                parsedEvents = Integer.parseInt(args[0]);
                if (parsedEvents <= 0) {
                    System.out.println("Number of events must be positive. Using default: " + DEFAULT_NUM_EVENTS);
                    parsedEvents = DEFAULT_NUM_EVENTS;
                }
            } catch (NumberFormatException e) {
                System.out.println("Invalid number format. Using default number of events: " + DEFAULT_NUM_EVENTS);
            }
        }
        final int numEvents = parsedEvents;

        System.out.println("Preparing to send " + numEvents + " random events to Kafka topic: " + INPUT_TOPIC);

        // 1. Initialize Fury using FurySerializationUtils for consistent configuration
        Fury fury = FurySerializationUtils.createConfiguredFury();
        // 2. Set up Kafka Producer properties
        Properties producerProps = new Properties();
        producerProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        producerProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        producerProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());

        // 3. Send multiple random events to the Kafka topic
        AtomicInteger successCount = new AtomicInteger(0);
        AtomicInteger failureCount = new AtomicInteger(0);


        try (Producer<String, byte[]> producer = new KafkaProducer<>(producerProps)) {
            for (int i = 0; i < numEvents; i++) {
                // Create a random event
                InteractionEvent event = createRandomEvent();

                // Serialize the event
                byte[] serializedEvent = fury.serialize(event);

                // Send the event to Kafka
                ProducerRecord<String, byte[]> record = new ProducerRecord<>(INPUT_TOPIC, null, serializedEvent);
                final int eventNumber = i + 1;
                producer.send(record, (metadata, exception) -> {
                    if (exception == null) {
                        int count = successCount.incrementAndGet();
                        if (count % 10 == 0 || count == numEvents) { // Log every 10 events or the last one
                            System.out.printf("Successfully sent %d/%d events to topic %s%n",
                                    count, numEvents, metadata.topic());
                        }
                    } else {
                        failureCount.incrementAndGet();
                        System.err.println("Failed to send message #" + eventNumber);
                        exception.printStackTrace();
                    }
                });

                // Add a small delay between events to avoid overwhelming the system
                if (i > 0 && i % 100 == 0) {
                    producer.flush();
                    System.out.println("Sent " + i + " events so far. Pausing briefly...");
                    try {
                        Thread.sleep(100); // 500ms pause every 100 events
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }

            producer.flush();
            System.out.println("Completed sending events. Success: " + successCount.get() + ", Failures: " + failureCount.get());
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
            int receivedCount = 0;
            int attempts = 0;
            int maxAttempts = 10; // Increase attempts for multiple events
            InteractionEvent processedEvent=null;
            while (receivedCount <= Math.min(numEvents, 100000) && attempts < maxAttempts) { // Limit to 10 events to avoid too much output
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));
                if (records.isEmpty()) {
                    System.out.println("No records received yet. Waiting... (Attempt " + (++attempts) + "/" + maxAttempts + ")");
                } else {
                    for (ConsumerRecord<String, byte[]> record : records) {
                        try {
                            // 9. Deserialize the processed event
                             processedEvent = (InteractionEvent) fury.deserialize(record.value());
                            receivedCount++;
                            System.out.println("\nReceived processed event #" + receivedCount + ":");
                            System.out.println("  User ID: " + processedEvent.getUserId().getUserId());
                            System.out.println("  Movie ID: " + processedEvent.getMovieId().getMovieId());
                            System.out.println("  Event Type: " + processedEvent.getDetails().getEventType());
                            System.out.println("  Processed: " + processedEvent.isProcessed());
                            System.out.println("  Timestamp: " + processedEvent.getDetails().getTimestamp());

                            // Break after receiving a reasonable number of events to avoid too much output
                            if (receivedCount >= Math.min(numEvents, 100000)) {
                                break;
                            }
                        } catch (Exception e) {
                            System.err.println("Failed to deserialize processed event");
                            e.printStackTrace();
                        }
                    }
                }
            }

            if (receivedCount == 0) {
                System.out.println("No processed events received after " + maxAttempts + " attempts. Please check if the processing service is running.");
            } else {
                System.out.println("\nReceived " + receivedCount + " processed events.");

                // After receiving processed events, listen for recommendations
                System.out.println("\nWaiting for recommendations on topic: " + RECOMMENDATIONS_TOPIC);
                listenForRecommendations(fury,processedEvent.getUserId().getUserId() ,numEvents);
            }
        }
    }

    /**
     * Listens for recommendations on the recommendations topic.
     *
     * @param fury The Fury instance for deserialization
     * @param userId The user ID to filter recommendations for
     */
    private static void listenForRecommendations(Fury fury, long userId, int numEvents) {
        // Set up Kafka Consumer properties for recommendations
        Properties recommendationsConsumerProps = new Properties();
        recommendationsConsumerProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        recommendationsConsumerProps.put(ConsumerConfig.GROUP_ID_CONFIG, "recommendations-reader-group");
        recommendationsConsumerProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        recommendationsConsumerProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        recommendationsConsumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, byte[]> consumer = new KafkaConsumer<>(recommendationsConsumerProps)) {
            consumer.subscribe(Collections.singletonList(RECOMMENDATIONS_TOPIC));

            int receivedCount = 0;
            int attempts = 0;
            int maxAttempts = 10; // Increase attempts for multiple recommendations

            System.out.println("Looking for recommendations for user ID: " + userId);

            while (receivedCount <=  numEvents && attempts < maxAttempts) { // Limit to 10 recommendation sets
                ConsumerRecords<String, byte[]> records = consumer.poll(Duration.ofMillis(POLL_TIMEOUT_MS));

                if (records.isEmpty()) {
                    System.out.println("No recommendations received yet. Waiting... (Attempt " + (++attempts) + "/" + maxAttempts + ")");
                } else {
                    for (ConsumerRecord<String, byte[]> record : records) {
                        try {
                            // Deserialize the recommendations
                            @SuppressWarnings("unchecked")
                            List<MovieRecommendation> recommendations = (List<MovieRecommendation>) fury.deserialize(record.value());

                            // Check if this recommendation is for our user (if key is present)
                            boolean isForTargetUser = (record.key() != null && record.key().equals(String.valueOf(userId)));
                            String userInfo = isForTargetUser ?
                                "for target user " + userId :
                                (record.key() != null ? "for user " + record.key() : "with no user key");

                            receivedCount++;
                            System.out.println("\nReceived recommendation set #" + receivedCount + " " + userInfo + ":");
                            System.out.println("--------------------------------------------");

                            if (recommendations.isEmpty()) {
                                System.out.println("No recommendations available in this set.");
                            } else {
                                for (int i = 0; i < recommendations.size(); i++) {
                                    MovieRecommendation rec = recommendations.get(i);
                                    System.out.printf("%d. Movie ID: %d, Rating: %.2f%n",
                                            i + 1, rec.getMovieId().getMovieId(), rec.getPredictedRating());
                                }
                            }

                            // If we've received enough recommendation sets, break out
                            if (receivedCount >= numEvents) {
                                break;
                            }
                        } catch (Exception e) {
                            System.err.println("Failed to deserialize recommendations");
                            e.printStackTrace();
                        }
                    }
                }
            }

            if (receivedCount == 0) {
                System.out.println("No recommendations received after " + maxAttempts + " attempts. The system might not be generating recommendations for this user or event type.");
            } else {
                System.out.println("\nReceived " + receivedCount + " recommendation sets for user " + userId + ".");
            }
        }
    }
}
