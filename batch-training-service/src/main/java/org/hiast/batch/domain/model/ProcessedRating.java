package org.hiast.batch.domain.model;


import org.hiast.ids.MovieId;
import org.hiast.ids.UserId;
import org.hiast.model.RatingValue;

import javax.management.MXBean;
import java.io.Serializable;
import java.time.Instant;
import java.util.Objects;

/**
 * Represents a rating after initial processing within the batch service,
 * ready for ALS model training.
 */
public class ProcessedRating implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int userId;
    private final int movieId;
    private final double ratingActual;
    private final long timestampEpochSeconds;

    /**
     * Constructor for ProcessedRating.
     *
     * @param userId      The user ID object.
     * @param movieId     The movie ID object.
     * @param ratingValue The rating value object.
     * @param timestamp   The original timestamp of the rating.
     */
    public ProcessedRating(UserId userId, MovieId movieId, RatingValue ratingValue, Instant timestamp) {
        this.userId = Objects.requireNonNull(userId, "UserId cannot be null").getUserId();
        this.movieId = Objects.requireNonNull(movieId, "MovieId cannot be null").getMovieId();
        this.ratingActual = Objects.requireNonNull(ratingValue, "RatingValue cannot be null").getRatingActual();
        this.timestampEpochSeconds = Objects.requireNonNull(timestamp, "Timestamp cannot be null").getEpochSecond();
    }

    public long getTimestampEpochSeconds() {
        return timestampEpochSeconds;
    }

    public int getUserId() {
        return userId;
    }

    public int getMovieId() {
        return movieId;
    }

    public double getRatingActual() {
        return ratingActual;
    }



    public RatingValue getRatingValue() {
        return RatingValue.of(this.ratingActual);
    }

    public Instant getTimestamp() {
        return Instant.ofEpochSecond(this.timestampEpochSeconds);
    }


    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ProcessedRating that = (ProcessedRating) o;
        return userId == that.userId &&
                movieId == that.movieId &&
                Double.compare(that.ratingActual, ratingActual) == 0 &&
                timestampEpochSeconds == that.timestampEpochSeconds;
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, movieId, ratingActual, timestampEpochSeconds);
    }

    @Override
    public String toString() {
        return "ProcessedRating{" +
                "userId=" + userId +
                ", movieId=" + movieId +
                ", ratingActual=" + ratingActual +
                ", timestampEpochSeconds=" + timestampEpochSeconds +
                '}';
    }
}
