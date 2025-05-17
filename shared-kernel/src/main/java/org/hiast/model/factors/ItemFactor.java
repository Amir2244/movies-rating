package org.hiast.model.factors;

import org.hiast.ids.MovieId; // Assuming MovieId is in shared-kernel

import java.util.Arrays;
import java.util.Objects;

/**
 * Represents an item's (e.g., movie's) feature factor.
 * It encapsulates an item's ID and its corresponding feature vector.
 *
 * @param <F> The type of the feature vector (e.g., float[], List<Float>).
 */
public class ItemFactor<F> implements Factor<MovieId, F> {

    private static final long serialVersionUID = 1L; // For Serializable

    private final MovieId itemId; // Using MovieId as the item identifier type
    private final F features;

    /**
     * Constructs a new ItemFactor.
     *
     * @param itemId The ID of the item. Must not be null.
     * @param features The feature vector for the item. Must not be null.
     */
    public ItemFactor(MovieId itemId, F features) {
        Objects.requireNonNull(itemId, "itemId cannot be null");
        Objects.requireNonNull(features, "features cannot be null");
        this.itemId = itemId;
        this.features = features;
    }

    @Override
    public MovieId getId() {
        return itemId;
    }

    @Override
    public F getFeatures() {
        return features;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ItemFactor<?> that = (ItemFactor<?>) o;
        if (!itemId.equals(that.itemId)) return false;
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
        int result = itemId.hashCode();
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
        return "ItemFactor{" +
                "itemId=" + itemId +
                ", features=" + featuresString +
                '}';
    }
}
