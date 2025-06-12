package org.hiast.analyticsapi.adapter.out.persistence;

import org.hiast.model.AnalyticsType;
import org.hiast.model.DataAnalytics;
import org.springframework.stereotype.Component;

/**
 * Mapper for converting between domain models and MongoDB documents.
 * Handles the transformation between the hexagonal architecture layers.
 */
@Component
public class AnalyticsDocumentMapper {

    /**
     * Converts from MongoDB document to domain model.
     */
    public DataAnalytics toDomain(AnalyticsDocument document) {
        if (document == null) {
            return null;
        }

        AnalyticsType type;
        try {
            type = AnalyticsType.valueOf(document.getType());
        } catch (IllegalArgumentException e) {
            // Handle unknown types gracefully
            throw new IllegalStateException("Unknown analytics type: " + document.getType(), e);
        }

        return new DataAnalytics(
                document.getAnalyticsId(),
                document.getGeneratedAtAsInstant(),
                type,
                document.getMetrics(),
                document.getDescription()
        );
    }

    /**
     * Converts from domain model to MongoDB document.
     */
    public AnalyticsDocument toDocument(DataAnalytics analytics) {
        if (analytics == null) {
            return null;
        }

        AnalyticsDocument document = new AnalyticsDocument();
        document.setAnalyticsId(analytics.getAnalyticsId());
        document.setGeneratedAtFromInstant(analytics.getGeneratedAt());
        document.setType(analytics.getType().name());
        document.setMetrics(analytics.getMetrics());
        document.setDescription(analytics.getDescription());
        
        return document;
    }
} 