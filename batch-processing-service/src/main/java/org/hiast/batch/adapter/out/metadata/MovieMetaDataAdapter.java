package org.hiast.batch.adapter.out.metadata;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hiast.batch.application.port.out.MovieMetaDataPort;
import org.hiast.ids.MovieId;
import org.hiast.model.MovieMetaData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Adapter that implements MovieMetaDataPort using Spark Dataset as the data source.
 * This adapter extracts movie metadata from the loaded movies dataset.
 */
public class MovieMetaDataAdapter implements MovieMetaDataPort {
    private static final Logger log = LoggerFactory.getLogger(MovieMetaDataAdapter.class);

    private final Map<Integer, MovieMetaData> movieMetaDataCache;

    /**
     * Constructor that initializes the adapter with movie data from Spark Dataset.
     *
     * @param moviesDataset The Spark Dataset containing movie data with schema: movieId, title, genres
     */
    public MovieMetaDataAdapter(Dataset<Row> moviesDataset) {
        this.movieMetaDataCache = new HashMap<>();
        loadMovieMetaData(moviesDataset);
    }

    /**
     * Loads movie metadata from the Spark Dataset into an in-memory cache.
     */
    private void loadMovieMetaData(Dataset<Row> moviesDataset) {
        if (moviesDataset == null || moviesDataset.isEmpty()) {
            log.warn("Movies dataset is null or empty. Movie metadata will not be available.");
            return;
        }

        try {
            log.info("Loading movie metadata from Spark dataset...");
            List<Row> movieRows = moviesDataset.collectAsList();
            
            for (Row row : movieRows) {
                try {
                    // Extract data from the row
                    String movieIdStr = row.getAs("movieId");
                    String title = row.getAs("title");
                    String genresStr = row.getAs("genres");

                    // Parse movieId
                    int movieId = Integer.parseInt(movieIdStr);

                    // Parse genres (assuming they are pipe-separated like "Action|Adventure|Sci-Fi")
                    List<String> genres = parseGenres(genresStr);

                    // Create MovieMetaData object
                    MovieMetaData metaData = new MovieMetaData(MovieId.of(movieId), title, genres);
                    movieMetaDataCache.put(movieId, metaData);

                } catch (Exception e) {
                    log.warn("Failed to parse movie row: {}. Error: {}", row, e.getMessage());
                }
            }

            log.info("Successfully loaded {} movie metadata entries", movieMetaDataCache.size());

        } catch (Exception e) {
            log.error("Error loading movie metadata from Spark dataset: {}", e.getMessage(), e);
        }
    }

    /**
     * Parses the genres string into a list of individual genres.
     * Handles the MovieLens format where genres are pipe-separated.
     */
    private List<String> parseGenres(String genresStr) {
        if (genresStr == null || genresStr.trim().isEmpty() || "(no genres listed)".equals(genresStr)) {
            return Collections.emptyList();
        }

        return Arrays.stream(genresStr.split("\\|"))
                .map(String::trim)
                .filter(genre -> !genre.isEmpty())
                .collect(Collectors.toList());
    }

    @Override
    public Optional<MovieMetaData> getMovieMetaData(int movieId) {
        return Optional.ofNullable(movieMetaDataCache.get(movieId));
    }

    @Override
    public Map<Integer, MovieMetaData> getMovieMetaDataBatch(List<Integer> movieIds) {
        Map<Integer, MovieMetaData> result = new HashMap<>();
        
        for (Integer movieId : movieIds) {
            MovieMetaData metaData = movieMetaDataCache.get(movieId);
            if (metaData != null) {
                result.put(movieId, metaData);
            }
        }
        
        return result;
    }

    @Override
    public boolean hasMovieMetaData(int movieId) {
        return movieMetaDataCache.containsKey(movieId);
    }

    /**
     * Gets the total number of movies in the metadata cache.
     *
     * @return The number of movies with metadata available.
     */
    public int getMovieCount() {
        return movieMetaDataCache.size();
    }

    /**
     * Gets all available movie IDs.
     *
     * @return Set of all movie IDs that have metadata available.
     */
    public Set<Integer> getAvailableMovieIds() {
        return new HashSet<>(movieMetaDataCache.keySet());
    }
}
