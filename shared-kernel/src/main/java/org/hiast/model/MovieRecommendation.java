package org.hiast.model;

import org.hiast.ids.MovieId;
import org.hiast.ids.UserId;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Shared domain model representing a movie recommendation.
 * This model uses value objects for type safety and is shared between
 * the batch training service and recommendations API.
 */
public final class MovieRecommendation implements Serializable {
    private static final long serialVersionUID = 1L;

    private final UserId userId;
    private final MovieId movieId;
    private final float predictedRating;
    private final Instant generatedAt;
    private final MovieMetaData movieMetaData;
    /**
     * Constructor for MovieRecommendation.
     *
     * @param userId          The user ID value object.
     * @param movieId         The movie ID value object.
     * @param predictedRating The predicted rating for this movie.
     * @param generatedAt     The timestamp when recommendation was generated.
     */
    public MovieRecommendation(UserId userId,
                               MovieId movieId,
                               float predictedRating,
                               Instant generatedAt, MovieMetaData movieMetaData) {
        this.userId = Objects.requireNonNull(userId, "userId cannot be null");
        this.movieId = Objects.requireNonNull(movieId, "movieId cannot be null");
        this.predictedRating = predictedRating;
        this.generatedAt = Objects.requireNonNull(generatedAt, "generatedAt cannot be null");
        this.movieMetaData = movieMetaData;
    }

    /**
     * Factory method for creating recommendations from primitive values.
     * Useful for batch processing scenarios.
     */
    public static MovieRecommendation of(int userId, int movieId, float rating, Instant generatedAt,MovieMetaData metaData) {
        return new MovieRecommendation(
                UserId.of(userId),
                MovieId.of(movieId),
                rating,
                generatedAt,
                metaData
        );
    }

    public UserId getUserId() {
        return userId;
    }

    public MovieId getMovieId() {
        return movieId;
    }

    public float getPredictedRating() {
        return predictedRating;
    }

    public Instant getGeneratedAt() {
        return generatedAt;
    }
    public String getMovieTitle() {
        return movieMetaData != null ? movieMetaData.getTitle() : null;
    }

    public List<String> getMovieGenres() {
        return movieMetaData != null ? movieMetaData.getGenres() : null;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MovieRecommendation that = (MovieRecommendation) o;
        return Float.compare(that.predictedRating, predictedRating) == 0 &&
                Objects.equals(userId, that.userId) &&
                Objects.equals(movieId, that.movieId) &&
                Objects.equals(generatedAt, that.generatedAt);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, movieId, predictedRating, generatedAt);
    }

    @Override
    public String toString() {
        return "MovieRecommendation{" +
                "userId=" + userId +
                ", movieId=" + movieId +
                ", predictedRating=" + predictedRating +
                ", generatedAt=" + generatedAt +
                '}';
    }
}