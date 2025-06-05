package org.hiast.batch.domain.model;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

/**
 * Represents movie metadata including title and genres.
 * This domain model encapsulates movie information used for enriching recommendations.
 */
public class MovieMetaData implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int movieId;
    private final String title;
    private final List<String> genres;

    /**
     * Constructor for MovieMetaData.
     *
     * @param movieId The movie ID.
     * @param title   The movie title.
     * @param genres  The list of genres for the movie.
     */
    public MovieMetaData(int movieId, String title, List<String> genres) {
        this.movieId = movieId;
        this.title = Objects.requireNonNull(title, "title cannot be null");
        this.genres = Objects.requireNonNull(genres, "genres cannot be null");
    }

    public int getMovieId() {
        return movieId;
    }

    public String getTitle() {
        return title;
    }

    public List<String> getGenres() {
        return genres;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MovieMetaData that = (MovieMetaData) o;
        return movieId == that.movieId &&
                Objects.equals(title, that.title) &&
                Objects.equals(genres, that.genres);
    }

    @Override
    public int hashCode() {
        return Objects.hash(movieId, title, genres);
    }

    @Override
    public String toString() {
        return "MovieMetaData{" +
                "movieId=" + movieId +
                ", title='" + title + '\'' +
                ", genres=" + genres +
                '}';
    }
}
