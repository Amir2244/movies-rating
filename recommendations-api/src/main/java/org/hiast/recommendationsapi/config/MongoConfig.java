package org.hiast.recommendationsapi.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/**
 * Configuration properties for MongoDB collections and settings.
 */
@Configuration
@ConfigurationProperties(prefix = "app.mongodb")
public class MongoConfig {
    
    private String recommendationsCollection = "user_recommendations";
    private String analyticsCollection = "analytics_data";
    
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
