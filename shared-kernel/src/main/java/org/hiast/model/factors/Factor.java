package org.hiast.model.factors;

import java.io.Serializable;

/**
 * Base interface for a feature factor.
 * A factor typically consists of an identifier and a feature vector.
 *
 * @param <ID_TYPE> The type of the identifier (e.g., UserId, MovieId, String, Long).
 * @param <FEATURE_TYPE> The type of the feature vector (e.g., float[], double[], List<Float>).
 */
public interface Factor<ID_TYPE, FEATURE_TYPE> extends Serializable {

    /**
     * Gets the unique identifier for this factor.
     *
     * @return The identifier.
     */
    ID_TYPE getId();

    /**
     * Gets the feature vector.
     *
     * @return The feature vector.
     */
    FEATURE_TYPE getFeatures();
}
