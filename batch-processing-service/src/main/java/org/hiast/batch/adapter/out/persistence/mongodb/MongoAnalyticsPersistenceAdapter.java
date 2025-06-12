package org.hiast.batch.adapter.out.persistence.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import org.bson.Document;
import org.hiast.batch.application.port.out.AnalyticsPersistencePort;
import org.hiast.batch.config.MongoConfig;
import org.hiast.model.DataAnalytics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Date;
import java.util.Map;

/**
 * Adapter implementing AnalyticsPersistencePort to save analytics data to MongoDB.
 */
public class MongoAnalyticsPersistenceAdapter implements AnalyticsPersistencePort {
    private static final Logger log = LoggerFactory.getLogger(MongoAnalyticsPersistenceAdapter.class);

    private final MongoConfig mongoConfig;

    public MongoAnalyticsPersistenceAdapter(MongoConfig mongoConfig) {
        this.mongoConfig = mongoConfig;
        log.info("MongoAnalyticsPersistenceAdapter initialized with config: {}", mongoConfig);
    }

    @Override
    public boolean saveDataAnalytics(DataAnalytics dataAnalytics) {
        if (dataAnalytics == null) {
            log.warn("No analytics data to save. Skipping MongoDB persistence.");
            return false;
        }

        log.info("Saving analytics data to MongoDB: {}", dataAnalytics.getAnalyticsId());
        String uri = mongoConfig.getConnectionString();
        try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase database = mongoClient.getDatabase(mongoConfig.getDatabase());
            MongoCollection<Document> collection = database.getCollection(mongoConfig.getAnalyticsCollection());

            Document document = convertToDocument(dataAnalytics);

            // Use upsert to replace existing analytics with the same ID
            ReplaceOptions options = new ReplaceOptions().upsert(true);
            collection.replaceOne(
                    Filters.eq("analyticsId", dataAnalytics.getAnalyticsId()),
                    document,
                    options
            );

            log.info("Successfully saved analytics data to MongoDB: {}", dataAnalytics.getAnalyticsId());
            return true;
        } catch (Exception e) {
            log.error("Failed to save analytics data to MongoDB: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * Converts a DataAnalytics object to a MongoDB Document.
     */
    private Document convertToDocument(DataAnalytics dataAnalytics) {
        Document document = new Document();
        document.append("analyticsId", dataAnalytics.getAnalyticsId());
        document.append("generatedAt", Date.from(dataAnalytics.getGeneratedAt()));
        document.append("type", dataAnalytics.getType().name());
        document.append("description", dataAnalytics.getDescription());

        // Convert metrics map to a document
        Document metricsDoc = new Document();
        for (Map.Entry<String, Object> entry : dataAnalytics.getMetrics().entrySet()) {
            metricsDoc.append(entry.getKey(), entry.getValue());
        }
        document.append("metrics", metricsDoc);

        return document;
    }
}
