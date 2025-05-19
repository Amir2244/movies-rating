package org.hiast.batch.config;

/**
 * Configuration for MongoDB connection.
 */
public class MongoConfig {
    private final String host;
    private final int port;
    private final String database;
    private final String recommendationsCollection;
    private final String analyticsCollection;

    public MongoConfig(String host, int port, String database, String recommendationsCollection, String analyticsCollection) {
        this.host = host;
        this.port = port;
        this.database = database;
        this.recommendationsCollection = recommendationsCollection;
        this.analyticsCollection = analyticsCollection;
    }

    public String getHost() {
        return host;
    }

    public int getPort() {
        return port;
    }

    public String getDatabase() {
        return database;
    }

    public String getRecommendationsCollection() {
        return recommendationsCollection;
    }

    public String getAnalyticsCollection() {
        return analyticsCollection;
    }

    public String getConnectionString() {
        return "mongodb://" + host + ":" + port;
    }

    @Override
    public String toString() {
        return "MongoConfig{" +
                "host='" + host + '\'' +
                ", port=" + port +
                ", database='" + database + '\'' +
                ", recommendationsCollection='" + recommendationsCollection + '\'' +
                ", analyticsCollection='" + analyticsCollection + '\'' +
                '}';
    }
}
