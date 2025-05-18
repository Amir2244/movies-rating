package org.hiast.batch.application.pipeline.filters;

import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hiast.batch.application.pipeline.ALSTrainingPipelineContext;
import org.hiast.batch.application.pipeline.Filter;
import org.hiast.batch.application.port.out.ResultPersistencePort;
import org.hiast.batch.domain.model.MovieRecommendation;
import org.hiast.batch.domain.model.UserRecommendations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Filter that saves the recommendation results to the persistence store.
 */
public class ResultSavingFilter implements Filter<ALSTrainingPipelineContext, ALSTrainingPipelineContext> {
    private static final Logger log = LoggerFactory.getLogger(ResultSavingFilter.class);
    private static final int NUM_RECOMMENDATIONS_PER_USER = 10;

    private final ResultPersistencePort resultPersistence;

    public ResultSavingFilter(ResultPersistencePort resultPersistence) {
        this.resultPersistence = resultPersistence;
    }

    @Override
    public ALSTrainingPipelineContext process(ALSTrainingPipelineContext context) {
        log.info("Saving recommendation results...");

        ALSModel model = context.getModel();

        if (model == null) {
            log.error("Model is null. Cannot generate and save recommendations.");
            throw new RuntimeException("Model is null. Cannot generate and save recommendations.");
        }

        try {
            // Generate recommendations for all users
            log.info("Generating top {} recommendations for all users...", NUM_RECOMMENDATIONS_PER_USER);
            Dataset<Row> userRecommendations = model.recommendForAllUsers(NUM_RECOMMENDATIONS_PER_USER);

            log.info("User recommendations schema:");
            userRecommendations.printSchema();
            log.info("Sample of user recommendations (first 5 rows, truncate=false):");
            userRecommendations.show(5, false);

            // Convert Spark Dataset to domain model objects
            List<UserRecommendations> userRecommendationsList = convertToUserRecommendations(userRecommendations);
            log.info("Converted {} user recommendations to domain model objects", userRecommendationsList.size());

            // Save to persistence store
            boolean saved = resultPersistence.saveUserRecommendations(userRecommendationsList);
            if (saved) {
                log.info("Recommendation results saved successfully.");
                context.setResultsSaved(true);
            } else {
                log.warn("Failed to save recommendation results.");
                context.setResultsSaved(false);
            }

        } catch (Exception e) {
            log.error("Error saving recommendation results: {}", e.getMessage(), e);
            throw new RuntimeException("Error saving recommendation results", e);
        }

        return context;
    }

    /**
     * Converts a Spark Dataset of user recommendations to a list of UserRecommendations domain objects.
     */
    private List<UserRecommendations> convertToUserRecommendations(Dataset<Row> userRecommendationsDataset) {
        List<UserRecommendations> result = new ArrayList<>();
        Instant now = Instant.now();
        String modelVersion = "ALS-" + UUID.randomUUID().toString().substring(0, 8);

        // Collect the dataset to the driver (be careful with large datasets)
        List<Row> rows = userRecommendationsDataset.collectAsList();

        for (Row row : rows) {
            int userId = row.getInt(0);
            List<Row> recommendations = row.getList(1);

            List<MovieRecommendation> movieRecommendations = new ArrayList<>();
            for (Row rec : recommendations) {
                int movieId = rec.getInt(0);
                float rating = (float) rec.getDouble(1);

                MovieRecommendation movieRec = new MovieRecommendation(userId, movieId, rating, now);
                movieRecommendations.add(movieRec);
            }

            UserRecommendations userRecs = new UserRecommendations(userId, movieRecommendations, now, modelVersion);
            result.add(userRecs);
        }

        return result;
    }
}
