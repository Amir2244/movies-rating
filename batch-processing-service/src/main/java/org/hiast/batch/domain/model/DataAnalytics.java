package org.hiast.batch.domain.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * Represents analytics data about user interactions and movie ratings.
 * This is used to store useful metrics for data analytics purposes.
 */
public class DataAnalytics implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String analyticsId;
    private final Instant generatedAt;
    private final AnalyticsType type;
    private final Map<String, Object> metrics;
    private final String description;



    /**
     * Constructor for DataAnalytics.
     *
     * @param analyticsId  The unique identifier for this analytics record.
     * @param generatedAt  The timestamp when the analytics were generated.
     * @param type         The type of analytics data.
     * @param metrics      The analytics metrics as key-value pairs.
     * @param description  A description of what these analytics represent.
     */
    public DataAnalytics(String analyticsId, 
                         Instant generatedAt, 
                         AnalyticsType type,
                         Map<String, Object> metrics,
                         String description) {
        this.analyticsId = Objects.requireNonNull(analyticsId, "analyticsId cannot be null");
        this.generatedAt = Objects.requireNonNull(generatedAt, "generatedAt cannot be null");
        this.type = Objects.requireNonNull(type, "type cannot be null");
        this.metrics = Objects.requireNonNull(metrics, "metrics cannot be null");
        this.description = description;
    }

    public String getAnalyticsId() {
        return analyticsId;
    }

    public Instant getGeneratedAt() {
        return generatedAt;
    }

    public AnalyticsType getType() {
        return type;
    }

    public Map<String, Object> getMetrics() {
        return metrics;
    }

    public String getDescription() {
        return description;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        DataAnalytics that = (DataAnalytics) o;
        return Objects.equals(analyticsId, that.analyticsId) &&
                Objects.equals(generatedAt, that.generatedAt) &&
                type == that.type &&
                Objects.equals(metrics, that.metrics) &&
                Objects.equals(description, that.description);
    }

    @Override
    public int hashCode() {
        return Objects.hash(analyticsId, generatedAt, type, metrics, description);
    }

    @Override
    public String toString() {
        return "DataAnalytics{" +
                "analyticsId='" + analyticsId + '\'' +
                ", generatedAt=" + generatedAt +
                ", type=" + type +
                ", metrics=" + metrics +
                ", description='" + description + '\'' +
                '}';
    }
}
