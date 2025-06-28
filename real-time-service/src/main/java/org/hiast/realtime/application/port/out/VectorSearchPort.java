
package org.hiast.realtime.application.port.out;

import org.hiast.ids.MovieId;
import org.hiast.model.MovieRecommendation;
import org.hiast.model.factors.UserFactor;
import java.util.List;

/**
 * Output Port for performing vector searches.
 * The adapter will implement this to query Redis for similar items.
 */
public interface VectorSearchPort {
    /**
     * Finds similar items based on user factor.
     * 
     * @param userFactor The user factor to find similar items for
     * @param topN The number of recommendations to return
     * @param eventWeight The weight of the event that triggered this search, used to adjust ratings
     * @return A list of movie recommendations
     */
    List<MovieRecommendation> findSimilarItems(UserFactor<float[]> userFactor, int topN, double eventWeight);

    /**
     * Finds similar items based on user factor (without considering event weight).
     * 
     * @param userFactor The user factor to find similar items for
     * @param topN The number of recommendations to return
     * @return A list of movie recommendations
     */
    default List<MovieRecommendation> findSimilarItems(UserFactor<float[]> userFactor, int topN) {
        // Default implementation calls the weighted version with a neutral weight of 1.0
        return findSimilarItems(userFactor, topN, 1.0);
    }

    /**
     * Finds similar items based on a movie that the user interacted with.
     * This performs a vector search between the movie vector and other movie vectors in Redis.
     * 
     * @param userFactor The user factor for whom recommendations are being generated
     * @param movieId The ID of the movie the user interacted with
     * @param topN The number of recommendations to return
     * @param eventWeight The weight of the event that triggered this search, used to adjust ratings
     * @return A list of movie recommendations
     */
    List<MovieRecommendation> findSimilarItemsByMovie(UserFactor<float[]> userFactor, MovieId movieId, int topN, double eventWeight);
}
