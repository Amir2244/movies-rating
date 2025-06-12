package org.hiast.analyticsapi.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.data.mongodb.config.AbstractMongoClientConfiguration;
import org.springframework.data.mongodb.repository.config.EnableMongoRepositories;

/**
 * MongoDB configuration for analytics API.
 * Configures MongoDB repositories and client settings.
 */
/**
 * Configuration properties for MongoDB collections and settings.
 */
@Configuration
@ConfigurationProperties(prefix = "app.mongodb")
public class MongoConfig {

    private String recommendationsCollection = "user_recommendations";
    private String analyticsCollection = "analytics";

    public String getRecommendationsCollection() {
        return recommendationsCollection;
    }

    public void setRecommendationsCollection(String recommendationsCollection) {
        this.recommendationsCollection = recommendationsCollection;
    }

    public String getAnalyticsCollection() {
        return analyticsCollection;
    }

    public void setAnalyticsCollection(String analyticsCollection) {
        this.analyticsCollection = analyticsCollection;
    }
}
