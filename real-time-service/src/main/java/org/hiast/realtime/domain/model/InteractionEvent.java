package org.hiast.realtime.domain.model;

import org.hiast.ids.MovieId;
import org.hiast.ids.UserId;
import org.hiast.model.InteractionEventDetails;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Represents a user interaction event consumed from Kafka.
 * This is the core domain object for our real-time processing.
 */

public class InteractionEvent implements Serializable {
    private static final long serialVersionUID = 1L;
    private UserId userId;
    private MovieId movieId;
    private InteractionEventDetails details;
    private boolean processed = false; // Flag to indicate if the event has been processed

    public InteractionEvent() {
    }

    public InteractionEvent(UserId userId, MovieId movieId, InteractionEventDetails details) {
        this.userId = userId;
        this.movieId = movieId;
        this.details = details;
        this.processed = false;
    }

    public UserId getUserId() {
        return userId;
    }

    public void setUserId(UserId userId) {
        this.userId = userId;
    }

    public MovieId getMovieId() {
        return movieId;
    }

    public void setMovieId(MovieId movieId) {
        this.movieId = movieId;
    }

    public InteractionEventDetails getDetails() {
        return details;
    }

    public void setDetails(InteractionEventDetails details) {
        this.details = details;
    }

    /**
     * Checks if the event has been processed.
     * @return true if the event has been processed, false otherwise
     */
    public boolean isProcessed() {
        return processed;
    }

    /**
     * Sets the processed flag.
     * @param processed the processed flag value
     */
    public void setProcessed(boolean processed) {
        this.processed = processed;
    }
}
