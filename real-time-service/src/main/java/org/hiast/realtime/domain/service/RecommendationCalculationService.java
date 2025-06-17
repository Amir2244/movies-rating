package org.hiast.realtime.domain.service;

import org.hiast.model.MovieRecommendation;
import java.util.List;

/**
 * A simple domain service for calculating dot product, if needed.
 * For this use case, Redis handles the core vector search logic,
 * so this remains simple.
 */
public class RecommendationCalculationService {
    public double calculateSimilarity(float[] userVector, float[] itemVector) {
        if (userVector == null || itemVector == null || userVector.length != itemVector.length) {
            return 0.0;
        }
        double dotProduct = 0.0;
        for (int i = 0; i < userVector.length; i++) {
            dotProduct += userVector[i] * itemVector[i];
        }
        return dotProduct;
    }
}
