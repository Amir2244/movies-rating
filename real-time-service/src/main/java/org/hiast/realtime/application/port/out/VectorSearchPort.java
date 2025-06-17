
package org.hiast.realtime.application.port.out;

import org.hiast.model.MovieRecommendation;
import org.hiast.model.factors.UserFactor;
import java.util.List;

/**
 * Output Port for performing vector searches.
 * The adapter will implement this to query Redis for similar items.
 */
public interface VectorSearchPort {
    List<MovieRecommendation> findSimilarItems(UserFactor userFactor, int topN);
}
