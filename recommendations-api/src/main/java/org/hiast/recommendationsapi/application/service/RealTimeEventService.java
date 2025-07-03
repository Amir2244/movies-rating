package org.hiast.recommendationsapi.application.service;

import org.hiast.events.EventType;
import org.hiast.ids.MovieId;
import org.hiast.ids.UserId;
import org.hiast.model.InteractionEvent;
import org.hiast.model.InteractionEventDetails;
import org.hiast.model.MovieRecommendation;
import org.hiast.model.RatingValue;
import org.hiast.recommendationsapi.adapter.in.web.request.InteractionEventRequest;
import org.hiast.recommendationsapi.adapter.out.messaging.kafka.KafkaConsumerService;
import org.hiast.recommendationsapi.adapter.out.messaging.kafka.KafkaProducerService;
import org.springframework.stereotype.Service;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

@Service
public class RealTimeEventService {
    private final KafkaProducerService kafkaProducerService;
    private final KafkaConsumerService kafkaConsumerService;

    public RealTimeEventService(KafkaProducerService kafkaProducerService, KafkaConsumerService kafkaConsumerService) {
        this.kafkaProducerService = Objects.requireNonNull(kafkaProducerService);
        this.kafkaConsumerService = Objects.requireNonNull(kafkaConsumerService);
    }

    /**
     * Processes an interaction event and returns the processed event.
     * 
     * @param request The interaction event request
     * @return The processed event
     */
    public InteractionEvent processInteractionEvent(InteractionEventRequest request) {
        return processInteractionEventWithRecommendations(request).getEvent();
    }

    /**
     * Processes an interaction event and returns both the processed event and recommendations.
     * 
     * @param request The interaction event request
     * @return An EventWithRecommendations object containing the processed event and recommendations
     */
    public EventWithRecommendations processInteractionEventWithRecommendations(InteractionEventRequest request) {
        if (request.getUserId() <= 0) {
            throw new IllegalArgumentException("User ID must be positive.");
        }
        EventType eventType = EventType.fromString(request.getEventType());
        if (eventType == EventType.UNKNOWN) {
            throw new IllegalArgumentException("Invalid event type: " + request.getEventType());
        }
        Instant timestamp = request.getTimestamp() != null ? request.getTimestamp() : Instant.now();
        UserId userId = UserId.of(request.getUserId());
        InteractionEventDetails.Builder builder = new InteractionEventDetails.Builder(userId, eventType, timestamp);
        if (request.getMovieId() != null) {
            builder.withMovieId(MovieId.of(request.getMovieId()));
        }
        if (eventType == EventType.RATING_GIVEN) {
            if (request.getRating() == null) {
                throw new IllegalArgumentException("Rating is required for RATING_GIVEN event.");
            }
            builder.withRatingValue(request.getRating());
        }
        if (request.getEventContext() != null) {
            builder.withEventContext(request.getEventContext());
        }
        InteractionEventDetails details = builder.build();
        MovieId movieId = request.getMovieId() != null ? MovieId.of(request.getMovieId()) : null;
        InteractionEvent event = new InteractionEvent(userId, movieId, details);
        kafkaProducerService.sendInteractionEvent(event);

        // Mark the event as processed
        event.setProcessed(true);

        // Asynchronously consume recommendations for this user
        CompletableFuture<List<MovieRecommendation>> recommendationsFuture = 
            kafkaConsumerService.consumeRecommendationsAsync(userId.getUserId());

        // Try to get recommendations with a timeout
        List<MovieRecommendation> recommendations = null;
        try {
            recommendations = recommendationsFuture.get(10, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            // Log the exception but don't propagate it
            System.err.println("Failed to get recommendations: " + e.getMessage());
        }

        return new EventWithRecommendations(event, recommendations);
    }

    /**
     * A class that holds both an InteractionEvent and a list of MovieRecommendations.
     */
    public static class EventWithRecommendations {
        private final InteractionEvent event;
        private final List<MovieRecommendation> recommendations;

        public EventWithRecommendations(InteractionEvent event, List<MovieRecommendation> recommendations) {
            this.event = event;
            this.recommendations = recommendations;
        }

        public InteractionEvent getEvent() {
            return event;
        }

        public List<MovieRecommendation> getRecommendations() {
            return recommendations;
        }
    }
} 
