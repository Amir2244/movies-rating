package org.hiast.analyticsapi.adapter.out.persistence;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;

import java.time.Instant;
import java.util.Date;
import java.util.Map;

/**
 * MongoDB document representing analytics data.
 * Maps to the analytics collection in MongoDB.
 */
@Document(collection = "analytics")
public class AnalyticsDocument {

    @Id
    private String id;
    private String analyticsId;
    private Date generatedAt;
    private String type;
    private Map<String, Object> metrics;
    private String description;

    public AnalyticsDocument() {
        // Default constructor for MongoDB
    }

    public AnalyticsDocument(String analyticsId, Date generatedAt, String type, 
                           Map<String, Object> metrics, String description) {
        this.analyticsId = analyticsId;
        this.generatedAt = generatedAt;
        this.type = type;
        this.metrics = metrics;
        this.description = description;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getAnalyticsId() {
        return analyticsId;
    }

    public void setAnalyticsId(String analyticsId) {
        this.analyticsId = analyticsId;
    }

    public Date getGeneratedAt() {
        return generatedAt;
    }

    public void setGeneratedAt(Date generatedAt) {
        this.generatedAt = generatedAt;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public Map<String, Object> getMetrics() {
        return metrics;
    }

    public void setMetrics(Map<String, Object> metrics) {
        this.metrics = metrics;
    }

    public String getDescription() {
        return description;
    }

    public void setDescription(String description) {
        this.description = description;
    }

    /**
     * Converts Date to Instant for domain model compatibility.
     */
    public Instant getGeneratedAtAsInstant() {
        return generatedAt != null ? generatedAt.toInstant() : null;
    }

    /**
     * Sets generatedAt from Instant for domain model compatibility.
     */
    public void setGeneratedAtFromInstant(Instant instant) {
        this.generatedAt = instant != null ? Date.from(instant) : null;
    }
} 