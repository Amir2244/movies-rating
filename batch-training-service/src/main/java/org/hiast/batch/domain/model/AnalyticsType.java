package org.hiast.batch.domain.model;

/**
 * Comprehensive types of analytics data that can be stored and tracked.
 * Organized by categories for better management and production readiness.
 */
public enum AnalyticsType {

    // === DATA QUALITY ANALYTICS ===
    /**
     * Analytics about data completeness, missing values, and data integrity.
     */
    DATA_COMPLETENESS("Data Quality", "Tracks completeness and integrity of rating data"),

    /**
     * Analytics about data freshness and recency of ratings.
     */
    DATA_FRESHNESS("Data Quality", "Monitors freshness and recency of rating data"),

    /**
     * Analytics encompassing overall data quality aspects including completeness, freshness, and consistency.
     */
    DATA_QUALITY("Data Quality", "Comprehensive data quality metrics"),

    // === USER BEHAVIOR ANALYTICS ===
    /**
     * Analytics about user activity patterns and rating behaviors.
     */
    USER_ACTIVITY("User Behavior", "Tracks user rating patterns and activity levels"),

    /**
     * Analytics about user engagement depth and interaction quality.
     */
    USER_ENGAGEMENT("User Behavior", "Measures user engagement depth and interaction quality"),
    /**
     * Analytics for user segmentation based on behavior patterns.
     */
    USER_SEGMENTATION("User Behavior", "Segments users based on rating behavior patterns"),

    // === CONTENT ANALYTICS ===
    /**
     * Analytics about movie popularity and rating frequency.
     */
    MOVIE_POPULARITY("Content Analytics", "Tracks movie popularity and rating frequency"),

    /**
     * Analytics about overall content performance and reception.
     */
    CONTENT_PERFORMANCE("Content Analytics", "Analyzes overall content performance metrics"),

    /**
     * Analytics about rating value distribution patterns.
     */
    RATING_DISTRIBUTION("Content Analytics", "Analyzes distribution of rating values"),

    /**
     * Analytics about genre preferences and distribution.
     */
    GENRE_DISTRIBUTION("Content Analytics", "Tracks genre preferences and distribution patterns"),

    /**
     * Analytics about temporal trends and seasonal patterns.
     */
    TEMPORAL_TRENDS("Content Analytics", "Identifies temporal trends and seasonal patterns"),

    // === SYSTEM PERFORMANCE ANALYTICS ===
    /**
     * Analytics about batch processing performance and efficiency.
     */
    PROCESSING_PERFORMANCE("System Performance", "Monitors batch processing performance metrics"),


    /**
     * Analytics about user satisfaction with recommendations.
     */
    USER_SATISFACTION("Business Metrics", "Tracks user satisfaction with recommendations");


    private final String category;
    private final String description;

    AnalyticsType(String category, String description) {
        this.category = category;
        this.description = description;
    }

    /**
     * Gets the category this analytics type belongs to.
     * @return The category name
     */
    public String getCategory() {
        return category;
    }

    /**
     * Gets the description of what this analytics type measures.
     * @return The description
     */
    public String getDescription() {
        return description;
    }

    /**
     * Checks if this analytics type is related to data quality.
     * @return true if this is a data quality analytics type
     */
    public boolean isDataQuality() {
        return "Data Quality".equals(category);
    }

    /**
     * Checks if this analytics type is related to user behavior.
     * @return true if this is a user behavior analytics type
     */
    public boolean isUserBehavior() {
        return "User Behavior".equals(category);
    }

    /**
     * Checks if this analytics type is related to content analytics.
     * @return true if this is a content analytics type
     */
    public boolean isContentAnalytics() {
        return "Content Analytics".equals(category);
    }

    /**
     * Checks if this analytics type is related to system performance.
     * @return true if this is a system performance analytics type
     */
    public boolean isSystemPerformance() {
        return "System Performance".equals(category);
    }

}