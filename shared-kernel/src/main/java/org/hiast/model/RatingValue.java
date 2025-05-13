package org.hiast.model;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents the ratingValue of a rating, ensuring it's within a valid range.
 * This is a Value Object.
 */
public final class RatingValue implements Serializable {
    private static final long serialVersionUID = 1L;
    public static final double MIN_RATING = 0.5;
    public static final double MAX_RATING = 5.0;

    private final double ratingValue;

    private RatingValue(double ratingValue) {
        this.ratingValue = ratingValue;
    }

    /**
     * Factory method to create a RatingValue.
     *
     * @param value The rating ratingValue.
     * @return A RatingValue instance.
     * @throws IllegalArgumentException if the ratingValue is outside the defined range.
     */
    public static RatingValue of(double value) {
        if (value < MIN_RATING || value > MAX_RATING) {
            throw new IllegalArgumentException(
                    String.format("Rating ratingValue must be between %.1f and %.1f, but was %.1f", MIN_RATING, MAX_RATING, value)
            );
        }
        return new RatingValue(value);
    }

    public double getRatingActual() {
        return ratingValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RatingValue that = (RatingValue) o;
        return Double.compare(that.ratingValue, ratingValue) == 0;
    }

    @Override
    public int hashCode() {
        return Objects.hash(ratingValue);
    }

    @Override
    public String toString() {
        return String.valueOf(ratingValue);
    }
}
