package org.hiast.analyticsapi.application.port.out;

import org.hiast.analyticsapi.domain.model.AnalyticsQuery;
import org.hiast.model.AnalyticsType;
import org.hiast.model.DataAnalytics;

import java.util.List;
import java.util.Map;
import java.util.Optional;

/**
 * Output port for loading analytics data from persistent storage.
 * This defines the contract for the infrastructure layer to implement.
 */
public interface LoadAnalyticsPort {

    /**
     * Loads analytics data based on the provided query criteria.
     *
     * @param query The query criteria for filtering analytics
     * @return List of analytics matching the criteria
     */
    List<DataAnalytics> loadAnalytics(AnalyticsQuery query);

    /**
     * Counts the total number of analytics records matching the query criteria.
     *
     * @param query The query criteria for filtering analytics
     * @return The total count of matching records
     */
    long countAnalytics(AnalyticsQuery query);

    /**
     * Loads a specific analytics record by its ID.
     *
     * @param analyticsId The unique identifier of the analytics record
     * @return The analytics record if found, empty otherwise
     */
    Optional<DataAnalytics> loadAnalyticsById(String analyticsId);

    /**
     * Loads counts of analytics by type.
     *
     * @return Map where key is the analytics type and value is the count
     */
    Map<AnalyticsType, Long> loadAnalyticsCountsByType();

    /**
     * Loads counts of analytics by category.
     *
     * @return Map where key is the category and value is the count
     */
    Map<String, Long> loadAnalyticsCountsByCategory();

    /**
     * Gets the latest analytics generation timestamp.
     *
     * @return The latest timestamp or null if no analytics exist
     */
    Optional<String> getLatestAnalyticsTimestamp();

    /**
     * Gets the oldest analytics generation timestamp.
     *
     * @return The oldest timestamp or null if no analytics exist
     */
    Optional<String> getOldestAnalyticsTimestamp();

    /**
     * Gets the total count of all analytics records.
     *
     * @return The total count of analytics records
     */
    long getTotalAnalyticsCount();

    /**
     * Gets the count of unique analytics types.
     *
     * @return The count of unique analytics types
     */
    long getUniqueAnalyticsTypesCount();

    /**
     * Checks if analytics data exists for a specific type.
     *
     * @param type The analytics type to check
     * @return true if analytics exist for the type, false otherwise
     */
    boolean existsByType(AnalyticsType type);

    /**
     * Loads the most recent analytics for each type.
     *
     * @return List of the most recent analytics for each type
     */
    List<DataAnalytics> loadLatestAnalyticsByType();
} 