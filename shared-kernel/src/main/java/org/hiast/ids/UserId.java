package org.hiast.ids;

import java.io.Serializable;
import java.util.Objects;

/**
 * Represents a unique identifier for a User.
 * This value object ensures type safety and encapsulates user ID logic.
 * Implements Serializable for use in distributed systems.
 */
public final class UserId implements Serializable {
    private static final long serialVersionUID = 1L;

    private final int userIdValue;

    /**
     * Private constructor to enforce creation via factory method or specific logic.
     * @param userIdValue The raw ID value.
     */
    private UserId(int userIdValue) {
        this.userIdValue = userIdValue;
    }

    /**
     * Factory method to create a UserId instance.
     * @param id The integer ID.
     * @return A new UserId instance.
     * @throws IllegalArgumentException if the userIdValue is invalid (e.g., non-positive).
     */
    public static UserId of(int id) {
        if (id <= 0) {
            throw new IllegalArgumentException("User ID must be positive.");
        }
        return new UserId(id);
    }

    /**
     * Gets the raw integer value of the user ID.
     * @return The user ID.
     */
    public int getUserId() {
        return userIdValue;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserId userId = (UserId) o;
        return userIdValue == userId.userIdValue;
    }

    @Override
    public int hashCode() {
        return Objects.hash(userIdValue);
    }

    @Override
    public String toString() {
        return "UserId{" +
                "userIdValue=" + userIdValue +
                '}';
    }
}
