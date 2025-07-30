package org.hiast.events;

/**
 * Enumeration for different types of user interaction events.
 * This helps in standardizing event types across the system.
 * Each event type has an associated weight that represents its importance
 * in the recommendation process.
 */
public enum EventType {
    RATING_GIVEN(5.0),       // User provided an explicit rating - highest weight
    MOVIE_VIEWED(3.0),       // User viewed a movie (or a significant portion) - high weight
    ADDED_TO_WATCHLIST(2.0), // User added a movie to their watchlist - medium weight
    MOVIE_CLICKED(1.0),      // User clicked on a movie for details - low weight
    SEARCHED_FOR_MOVIE(0.5), // User searched for a movie - lowest weight
    UNKNOWN(0.0);            // Default or fallback event type - no weight

    private final double weight;

    EventType(double weight) {
        this.weight = weight;
    }

    /**
     * Returns the weight of this event type.
     * @return the weight value
     */
    public double getWeight() {
        return weight;
    }

    /**
     * Parses a string to an EventType.
     * @param typeString The string representation of the event type.
     * @return The corresponding EventType, or UNKNOWN if not found.
     */
    public static EventType fromString(String typeString) {
        if (typeString == null) {
            return UNKNOWN;
        }
        try {
            return EventType.valueOf(typeString.toUpperCase().replace("-", "_"));
        } catch (IllegalArgumentException e) {
            return UNKNOWN;
        }
    }
}
