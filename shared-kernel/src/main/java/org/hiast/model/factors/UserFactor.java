package org.hiast.model.factors;

import org.hiast.ids.UserId; // Assuming UserId is in shared-kernel as per your project structure

import java.util.Arrays;
import java.util.Objects;

/**
 * Represents a user's feature factor, commonly used in recommendation models.
 * It encapsulates a user's ID and their corresponding feature vector.
 *
 * @param <F> The type of the feature vector (e.g., float[], List<Float>).
 */
public class UserFactor<F> implements Factor<UserId, F> {

    private static final long serialVersionUID = 1L; // For Serializable

    private final UserId userId;
    private final F features;

    /**
     * Constructs a new UserFactor.
     *
     * @param userId The ID of the user. Must not be null.
     * @param features The feature vector for the user. Must not be null.
     */
    public UserFactor(UserId userId, F features) {
        Objects.requireNonNull(userId, "userId cannot be null");
        Objects.requireNonNull(features, "features cannot be null");
        this.userId = userId;
        this.features = features;
    }

    @Override
    public UserId getId() {
        return userId;
    }

    @Override
    public F getFeatures() {
        return features;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserFactor<?> that = (UserFactor<?>) o;
        if (!userId.equals(that.userId)) return false;
        // For array features, deep equality is needed.
        if (features instanceof float[] && that.features instanceof float[]) {
            return Arrays.equals((float[]) features, (float[]) that.features);
        }
        if (features instanceof double[] && that.features instanceof double[]) {
            return Arrays.equals((double[]) features, (double[]) that.features);
        }
        return features.equals(that.features);
    }

    @Override
    public int hashCode() {
        int result = userId.hashCode();
        if (features instanceof float[]) {
            result = 31 * result + Arrays.hashCode((float[]) features);
        } else if (features instanceof double[]) {
            result = 31 * result + Arrays.hashCode((double[]) features);
        } else {
            result = 31 * result + features.hashCode();
        }
        return result;
    }

    @Override
    public String toString() {
        String featuresString;
        if (features instanceof float[]) {
            featuresString = Arrays.toString((float[]) features);
        } else if (features instanceof double[]) {
            featuresString = Arrays.toString((double[]) features);
        } else {
            featuresString = features.toString();
        }
        return "UserFactor{" +
                "userId=" + userId +
                ", features=" + featuresString +
                '}';
    }
}
