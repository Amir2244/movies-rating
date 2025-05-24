package org.hiast.batch.domain.model.analytics;

import java.util.HashMap;
import java.util.Map;

/**
 * Builder class for creating analytics metrics maps.
 * Provides a fluent API for building complex metrics objects
 * while ensuring type safety and validation.
 */
public class AnalyticsMetrics {
    
    private final Map<String, Object> metrics;
    
    private AnalyticsMetrics() {
        this.metrics = new HashMap<>();
    }
    
    /**
     * Creates a new AnalyticsMetrics builder.
     * 
     * @return A new builder instance
     */
    public static AnalyticsMetrics builder() {
        return new AnalyticsMetrics();
    }
    
    /**
     * Adds a string metric.
     * 
     * @param key The metric key
     * @param value The metric value
     * @return This builder for method chaining
     */
    public AnalyticsMetrics addMetric(String key, String value) {
        validateKey(key);
        metrics.put(key, value);
        return this;
    }
    
    /**
     * Adds a numeric metric.
     * 
     * @param key The metric key
     * @param value The metric value
     * @return This builder for method chaining
     */
    public AnalyticsMetrics addMetric(String key, Number value) {
        validateKey(key);
        metrics.put(key, value);
        return this;
    }
    
    /**
     * Adds a boolean metric.
     * 
     * @param key The metric key
     * @param value The metric value
     * @return This builder for method chaining
     */
    public AnalyticsMetrics addMetric(String key, Boolean value) {
        validateKey(key);
        metrics.put(key, value);
        return this;
    }
    
    /**
     * Adds multiple metrics from a map.
     * 
     * @param metricsMap The metrics to add
     * @return This builder for method chaining
     */
    public AnalyticsMetrics addMetrics(Map<String, Object> metricsMap) {
        if (metricsMap != null) {
            metricsMap.forEach((key, value) -> {
                validateKey(key);
                metrics.put(key, value);
            });
        }
        return this;
    }
    
    /**
     * Adds a metric conditionally.
     * 
     * @param condition The condition to check
     * @param key The metric key
     * @param value The metric value
     * @return This builder for method chaining
     */
    public AnalyticsMetrics addMetricIf(boolean condition, String key, Object value) {
        if (condition) {
            validateKey(key);
            metrics.put(key, value);
        }
        return this;
    }
    
    /**
     * Adds a metric with a default value if the provided value is null.
     * 
     * @param key The metric key
     * @param value The metric value
     * @param defaultValue The default value to use if value is null
     * @return This builder for method chaining
     */
    public AnalyticsMetrics addMetricWithDefault(String key, Object value, Object defaultValue) {
        validateKey(key);
        metrics.put(key, value != null ? value : defaultValue);
        return this;
    }
    
    /**
     * Builds and returns the metrics map.
     * 
     * @return An immutable copy of the metrics map
     */
    public Map<String, Object> build() {
        return new HashMap<>(metrics);
    }
    
    /**
     * Returns the current size of the metrics map.
     * 
     * @return The number of metrics
     */
    public int size() {
        return metrics.size();
    }
    
    /**
     * Checks if the metrics map is empty.
     * 
     * @return true if empty, false otherwise
     */
    public boolean isEmpty() {
        return metrics.isEmpty();
    }
    
    /**
     * Validates that the metric key is not null or empty.
     * 
     * @param key The key to validate
     * @throws IllegalArgumentException if the key is invalid
     */
    private void validateKey(String key) {
        if (key == null || key.trim().isEmpty()) {
            throw new IllegalArgumentException("Metric key cannot be null or empty");
        }
    }
}
