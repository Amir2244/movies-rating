package org.hiast.batch.application.service.factory;

import org.hiast.batch.application.service.analytics.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

/**
 * Factory for creating and managing analytics collectors.
 * Implements the Factory pattern to provide centralized creation
 * and configuration of analytics collectors.
 * <p>
 * This factory ensures proper ordering and initialization of
 * analytics collectors while maintaining loose coupling.
 */
public class AnalyticsCollectorFactory {

    private static final Logger log = LoggerFactory.getLogger(AnalyticsCollectorFactory.class);

    /**
     * Creates all available analytics collectors in priority order.
     *
     * @return List of analytics collectors sorted by priority
     */
    public static List<AnalyticsCollector> createAllCollectors() {
        log.info("Creating all analytics collectors...");

        List<AnalyticsCollector> collectors = new ArrayList<>();

        collectors.add(new UserAnalyticsService());
        collectors.add(new ContentAnalyticsService());
        collectors.add(new DataQualityAnalyticsService());
        collectors.add(new RatingDistributionAnalyticsService());
        collectors.add(new TemporalTrendsAnalyticsService());
        collectors.add(new TagAnalyticsService());
        collectors.add(new SystemPerformanceAnalyticsService());
        collectors.add(new DataFreshnessAnalyticsService());
        collectors.add(new UserSegmentationAnalyticsService());
        collectors.add(new UserEngagementAnalyticsService());
        // Sort by priority (lower numbers = higher priority)
        collectors.sort(Comparator.comparingInt(AnalyticsCollector::getPriority));

        log.info("Created {} analytics collectors", collectors.size());
        return collectors;
    }

    /**
     * Creates analytics collectors for specific types.
     *
     * @param analyticsTypes The types of analytics to create collectors for
     * @return List of matching analytics collectors
     */
    public static List<AnalyticsCollector> createCollectors(String... analyticsTypes) {
        List<AnalyticsCollector> allCollectors = createAllCollectors();
        List<AnalyticsCollector> filteredCollectors = new ArrayList<>();

        for (String type : analyticsTypes) {
            allCollectors.stream()
                    .filter(collector -> collector.getAnalyticsType().equalsIgnoreCase(type))
                    .findFirst()
                    .ifPresent(filteredCollectors::add);
        }

        log.info("Created {} filtered analytics collectors for types: {}",
                filteredCollectors.size(), String.join(", ", analyticsTypes));

        return filteredCollectors;
    }

    /**
     * Creates a specific analytics collector by type.
     *
     * @param analyticsType The type of analytics collector to create
     * @return The analytics collector, or null if not found
     */
    public static AnalyticsCollector createCollector(String analyticsType) {
        switch (analyticsType.toUpperCase()) {
            case "USER_ANALYTICS":
                return new UserAnalyticsService();
            case "CONTENT_ANALYTICS":
                return new ContentAnalyticsService();
            case "DATA_QUALITY":
                return new DataQualityAnalyticsService();
            case "RATING_DISTRIBUTION":
                return new RatingDistributionAnalyticsService();
            case "TEMPORAL_TRENDS":
                return new TemporalTrendsAnalyticsService();
            case "TAG_ANALYTICS":
                return new TagAnalyticsService();
            case "SYSTEM_PERFORMANCE":
                return new SystemPerformanceAnalyticsService();
            default:
                log.warn("Unknown analytics type: {}", analyticsType);
                return null;
        }
    }

    /**
     * Gets the count of available analytics collector types.
     *
     * @return The number of available collector types
     */
    public static int getAvailableCollectorCount() {
        return createAllCollectors().size();
    }
}
