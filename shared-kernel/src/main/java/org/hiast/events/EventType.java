package org.hiast.events;

/**
 * Enumeration for different types of user interaction events.
 * This helps in standardizing event types across the system.
 */
public enum EventType {
    RATING_GIVEN,       // User provided an explicit rating
    MOVIE_VIEWED,       // User viewed a movie (or a significant portion)
    MOVIE_CLICKED,      // User clicked on a movie for details
    ADDED_TO_WATCHLIST, // User added a movie to their watchlist
    SEARCHED_FOR_MOVIE, // User searched for a movie
    // Add other relevant event types as needed
    UNKNOWN;            // Default or fallback event type

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