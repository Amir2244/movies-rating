package org.hiast.model;


import org.hiast.events.EventType;
import org.hiast.ids.MovieId;
import org.hiast.ids.UserId;

import java.io.Serializable;
import java.time.Instant; // Using Instant for timestamps
import java.util.Objects;
import java.util.Optional;

/**
 * Represents the details of a user interaction event.
 * This is a core domain model.
 */
public class InteractionEventDetails implements Serializable {
    private static final long serialVersionUID = 1L;

    private final UserId userId;
    private final MovieId movieId;
    private final EventType eventType;
    private final Instant timestamp;
    private final Optional<RatingValue> ratingValue;
    private final Optional<String> eventContext;


    private InteractionEventDetails(Builder builder) {
        this.userId = Objects.requireNonNull(builder.userId, "userId cannot be null");
        this.movieId = builder.movieId;
        this.eventType = Objects.requireNonNull(builder.eventType, "eventType cannot be null");
        this.timestamp = Objects.requireNonNull(builder.timestamp, "timestamp cannot be null");
        this.ratingValue = Optional.ofNullable(builder.ratingValue);
        this.eventContext = Optional.ofNullable(builder.eventContext);
    }


    public UserId getUserId() {
        return userId;
    }

    public Optional<MovieId> getMovieId() {
        return Optional.ofNullable(movieId);
    }

    public EventType getEventType() {
        return eventType;
    }

    public Instant getTimestamp() {
        return timestamp;
    }

    public Optional<RatingValue> getRatingValue() {
        return ratingValue;
    }

    public Optional<String> getEventContext() {
        return eventContext;
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        InteractionEventDetails that = (InteractionEventDetails) o;
        return Objects.equals(userId, that.userId) &&
                Objects.equals(movieId, that.movieId) &&
                eventType == that.eventType &&
                Objects.equals(timestamp, that.timestamp) &&
                Objects.equals(ratingValue, that.ratingValue) &&
                Objects.equals(eventContext, that.eventContext);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, movieId, eventType, timestamp, ratingValue, eventContext);
    }

    @Override
    public String toString() {
        return "InteractionEventDetails{" +
                "userId=" + userId +
                ", movieId=" + movieId +
                ", eventType=" + eventType +
                ", timestamp=" + timestamp +
                ", ratingValue=" + ratingValue.map(RatingValue::toString).orElse("N/A") +
                ", eventContext=" + eventContext.orElse("N/A") +
                '}';
    }

    // Builder pattern for constructing InteractionEventDetails
    public static class Builder {
        private UserId userId;
        private MovieId movieId;
        private EventType eventType;
        private Instant timestamp;
        private RatingValue ratingValue;
        private String eventContext;

        public Builder(UserId userId, EventType eventType, Instant timestamp) {
            this.userId = userId;
            this.eventType = eventType;
            this.timestamp = timestamp;
        }

        public Builder withMovieId(MovieId movieId) {
            this.movieId = movieId;
            return this;
        }

        public Builder withRatingValue(RatingValue ratingValue) {
            if (this.eventType != EventType.RATING_GIVEN && ratingValue != null) {
                throw new IllegalArgumentException("RatingValue can only be set for RATING_GIVEN event type.");
            }
            this.ratingValue = ratingValue;
            return this;
        }

        public Builder withRatingValue(double rawRatingValue) {
            if (this.eventType != EventType.RATING_GIVEN) {
                throw new IllegalArgumentException("RatingValue can only be set for RATING_GIVEN event type.");
            }
            this.ratingValue = RatingValue.of(rawRatingValue);
            return this;
        }


        public Builder withEventContext(String eventContext) {
            this.eventContext = eventContext;
            return this;
        }

        public InteractionEventDetails build() {
            return new InteractionEventDetails(this);
        }
    }
}
