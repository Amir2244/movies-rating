package org.hiast.batch.application.pipeline.filters;

import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hiast.batch.adapter.out.metadata.MovieMetaDataAdapter;
import org.hiast.batch.application.pipeline.ALSTrainingPipelineContext;
import org.hiast.batch.application.pipeline.Filter;
import org.hiast.batch.application.port.out.MovieMetaDataPort;
import org.hiast.batch.application.port.out.ResultPersistencePort;
import org.hiast.batch.domain.exception.ModelPersistenceException;
import org.hiast.batch.domain.model.MovieMetaData;
import org.hiast.batch.domain.model.MovieRecommendation;
import org.hiast.batch.domain.model.UserRecommendations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Filter that enriches recommendation results with movie metadata and saves them to the persistence store.
 * This filter creates a MovieMetaDataAdapter from the loaded movie data and uses it to enrich recommendations.
 */
public class MovieMetaDataEnrichmentFilter implements Filter<ALSTrainingPipelineContext, ALSTrainingPipelineContext> {
    private static final Logger log = LoggerFactory.getLogger(MovieMetaDataEnrichmentFilter.class);
    private static final int NUM_RECOMMENDATIONS_PER_USER = 10;

    private final ResultPersistencePort resultPersistence;

    public MovieMetaDataEnrichmentFilter(ResultPersistencePort resultPersistence) {
        this.resultPersistence = resultPersistence;
    }

    @Override
    public ALSTrainingPipelineContext process(ALSTrainingPipelineContext context) {
        log.info("Enriching and saving recommendation results with movie metadata...");

        ALSModel model = context.getModel();

        if (model == null) {
            log.error("Model is null. Cannot generate and save recommendations.");
            throw new ModelPersistenceException("Model is null. Cannot generate and save recommendations. Check if model training completed successfully.");
        }

        try {
            // Create MovieMetaDataAdapter from the loaded movie data
            Dataset<Row> rawMovies = context.getRawMovies();
            MovieMetaDataPort movieMetaDataPort = new MovieMetaDataAdapter(rawMovies);
            log.info("Created MovieMetaDataAdapter with movie data");

            // Generate recommendations for all users
            log.info("Generating top {} recommendations for all users...", NUM_RECOMMENDATIONS_PER_USER);
            Dataset<Row> userRecommendations = model.recommendForAllUsers(NUM_RECOMMENDATIONS_PER_USER);

            log.info("User recommendations schema:");
            userRecommendations.printSchema();
            log.info("Sample of user recommendations (first 5 rows, truncate=false):");
            userRecommendations.show(5, false);

            // Convert Spark Dataset to domain model objects with movie metadata enrichment
            List<UserRecommendations> userRecommendationsList = convertToUserRecommendations(userRecommendations, movieMetaDataPort);
            log.info("Converted {} user recommendations to domain model objects with movie metadata", userRecommendationsList.size());

            // Save to persistence store
            boolean saved = resultPersistence.saveUserRecommendations(userRecommendationsList);
            if (saved) {
                log.info("Recommendation results saved successfully with movie metadata.");
                context.setResultsSaved(true);
            } else {
                log.warn("Failed to save recommendation results.");
                context.setResultsSaved(false);
            }

        } catch (Exception e) {
            log.error("Error enriching and saving recommendation results: {}", e.getMessage(), e);
            throw new ModelPersistenceException("Error enriching and saving recommendation results", e);
        }

        context.markResultSavingCompleted();
        return context;
    }

    /**
     * Converts a Spark Dataset of user recommendations to a list of UserRecommendations domain objects.
     * Enriches recommendations with movie metadata (title and genres) when available.
     */
    private List<UserRecommendations> convertToUserRecommendations(Dataset<Row> userRecommendationsDataset, 
                                                                  MovieMetaDataPort movieMetaDataPort) {
        List<UserRecommendations> result = new ArrayList<>();
        Instant now = Instant.now();
        String modelVersion = "ALS-" + UUID.randomUUID().toString().substring(0, 8);

        // Collect the dataset to the driver (be careful with large datasets)
        List<Row> rows = userRecommendationsDataset.collectAsList();

        // Collect all unique movie IDs for batch metadata lookup
        List<Integer> allMovieIds = rows.stream()
                .flatMap(row -> {
                    List<Row> recommendations = row.getList(1);
                    return recommendations.stream().map(rec -> rec.getInt(0));
                })
                .distinct()
                .collect(Collectors.toList());

        // Batch fetch movie metadata to avoid N+1 queries
        Map<Integer, MovieMetaData> movieMetaDataMap = movieMetaDataPort.getMovieMetaDataBatch(allMovieIds);
        log.info("Retrieved metadata for {} out of {} unique movies in recommendations", 
                movieMetaDataMap.size(), allMovieIds.size());

        for (Row row : rows) {
            int userId = row.getInt(0);
            List<Row> recommendations = row.getList(1);

            List<MovieRecommendation> movieRecommendations = new ArrayList<>();
            for (Row rec : recommendations) {
                int movieId = rec.getInt(0);
                float rating;
                try {
                    // Try to get as double first
                    rating = (float) rec.getDouble(1);
                } catch (ClassCastException e) {
                    // If that fails, try to get as float
                    try {
                        rating = rec.getFloat(1);
                    } catch (Exception e2) {
                        // As a last resort, get the object and convert it
                        Object ratingObj = rec.get(1);
                        if (ratingObj instanceof Double) {
                            rating = ((Double) ratingObj).floatValue();
                        } else if (ratingObj instanceof Float) {
                            rating = (Float) ratingObj;
                        } else if (ratingObj instanceof Integer) {
                            rating = ((Integer) ratingObj).floatValue();
                        } else {
                            rating = Float.parseFloat(ratingObj.toString());
                        }
                    }
                }

                // Get movie metadata if available
                MovieMetaData metaData = movieMetaDataMap.get(movieId);
                MovieRecommendation movieRec;
                
                if (metaData != null) {
                    // Create recommendation with movie metadata
                    movieRec = new MovieRecommendation(userId, movieId, rating, now, 
                                                    metaData);
                } else {
                    // Create recommendation without metadata
                    movieRec = new MovieRecommendation(userId, movieId, rating, now);
                    log.debug("No metadata found for movie ID: {}", movieId);
                }
                
                movieRecommendations.add(movieRec);
            }

            UserRecommendations userRecs = new UserRecommendations(userId, movieRecommendations, now, modelVersion);
            result.add(userRecs);
        }

        return result;
    }
}
