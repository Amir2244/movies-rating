package org.hiast.ids;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents a unique identifier for a Movie.
 * This value object ensures type safety and encapsulates movie ID logic.
 * Implements Serializable for use in distributed systems.
 */
public final class MovieId implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int movieIdValue; // Or String, or UUID

    private MovieId(int movieIdValue) {
        this.movieIdValue = movieIdValue;
    }

    /**
     * Factory method to create a MovieId instance.
     * @param id The integer ID.
     * @return A new MovieId instance.
     * @throws IllegalArgumentException if the movieIdValue is invalid.
     */
    public static MovieId of(int id) {
        if (id <= 0) {
            throw new IllegalArgumentException("Movie ID must be positive.");
        }
        return new MovieId(id);
    }

    public int getMovieId() {
        return movieIdValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MovieId movieId = (MovieId) o;
        return movieIdValue == movieId.movieIdValue;
    }

    @Override
    public int hashCode() {
        return Objects.hash(movieIdValue);
    }

    @Override
    public String toString() {
        return "MovieId{" +
                "movieIdValue=" + movieIdValue +
                '}';
    }
}
