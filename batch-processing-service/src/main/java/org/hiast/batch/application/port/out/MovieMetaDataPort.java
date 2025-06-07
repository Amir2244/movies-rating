package org.hiast.batch.application.port.out;

import org.hiast.model.MovieMetaData;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Output port for accessing movie metadata.
 * This port defines the contract for retrieving movie information
 * such as titles and genres for enriching recommendations.
 */
public interface MovieMetaDataPort {

    /**
     * Retrieves movie metadata by movie ID.
     *
     * @param movieId The movie ID to look up.
     * @return Optional containing the movie metadata if found, empty otherwise.
     */
    Optional<MovieMetaData> getMovieMetaData(int movieId);

    /**
     * Retrieves movie metadata for multiple movie IDs in batch.
     * This method is optimized for bulk operations to avoid N+1 query problems.
     *
     * @param movieIds The list of movie IDs to look up.
     * @return Map of movie ID to MovieMetaData for found movies.
     */
    Map<Integer, MovieMetaData> getMovieMetaDataBatch(List<Integer> movieIds);

    /**
     * Checks if movie metadata is available for the given movie ID.
     *
     * @param movieId The movie ID to check.
     * @return true if metadata exists, false otherwise.
     */
    boolean hasMovieMetaData(int movieId);
}
