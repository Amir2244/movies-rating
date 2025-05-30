package org.hiast.batch.adapter.out.persistence.mongodb;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.BulkWriteOptions;
import org.bson.Document;
import org.hiast.batch.application.port.out.ResultPersistencePort;
import org.hiast.batch.config.MongoConfig;
import org.hiast.batch.domain.model.MovieRecommendation;
import org.hiast.batch.domain.model.UserRecommendations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Adapter implementing ResultPersistencePort to save recommendation results to MongoDB.
 * Optimized with bulk operations and connection pooling.
 */
public class MongoResultPersistenceAdapter implements ResultPersistencePort {
    private static final Logger log = LoggerFactory.getLogger(MongoResultPersistenceAdapter.class);
    private static final int BATCH_SIZE = 1000;
    private static final int MAX_POOL_SIZE = 100;
    private static final int MIN_POOL_SIZE = 10;
    private static final int MAX_IDLE_TIME_MS = 60000;
    private static final int CONNECTION_TIMEOUT_MS = 30000;

    private final MongoConfig mongoConfig;
    private final MongoClient mongoClient;

    public MongoResultPersistenceAdapter(MongoConfig mongoConfig) {
        this.mongoConfig = mongoConfig;
        this.mongoClient = createMongoClient();
        log.info("MongoResultPersistenceAdapter initialized with config: {}", mongoConfig);
    }

    private MongoClient createMongoClient() {
        ConnectionString connectionString = new ConnectionString(mongoConfig.getConnectionString());
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .applyToConnectionPoolSettings(builder -> builder
                        .maxSize(MAX_POOL_SIZE)
                        .minSize(MIN_POOL_SIZE)
                        .maxConnectionIdleTime(MAX_IDLE_TIME_MS, TimeUnit.MILLISECONDS))
                .applyToSocketSettings(builder -> builder
                        .connectTimeout(CONNECTION_TIMEOUT_MS, TimeUnit.MILLISECONDS))
                .build();
        return MongoClients.create(settings);
    }

    @Override
    public boolean saveUserRecommendations(List<UserRecommendations> userRecommendationsList) {
        if (userRecommendationsList == null || userRecommendationsList.isEmpty()) {
            log.warn("No user recommendations to save. Skipping MongoDB persistence.");
            return false;
        }

        log.info("Saving {} user recommendations to MongoDB...", userRecommendationsList.size());
        try {
            MongoDatabase database = mongoClient.getDatabase(mongoConfig.getDatabase());
            MongoCollection<Document> collection = database.getCollection(mongoConfig.getRecommendationsCollection());

            List<WriteModel<Document>> bulkOperations = new ArrayList<>();
            int totalProcessed = 0;
            int successCount = 0;

            for (UserRecommendations userRecs : userRecommendationsList) {
                try {
                    Document document = convertToDocument(userRecs);
                    ReplaceOptions options = new ReplaceOptions().upsert(true);
                    bulkOperations.add(new ReplaceOneModel<>(
                            Filters.eq("userId", userRecs.getUserId()),
                            document,
                            options
                    ));

                    totalProcessed++;
                    if (bulkOperations.size() >= BATCH_SIZE) {
                        successCount += executeBulkOperation(collection, bulkOperations);
                        bulkOperations.clear();
                    }
                } catch (Exception e) {
                    log.error("Error preparing recommendations for user {}: {}",
                            userRecs != null ? userRecs.getUserId() : "unknown", e.getMessage(), e);
                }
            }

            // Execute remaining operations
            if (!bulkOperations.isEmpty()) {
                successCount += executeBulkOperation(collection, bulkOperations);
            }

            log.info("Successfully saved {}/{} user recommendations to MongoDB",
                    successCount, totalProcessed);

            return successCount > 0;
        } catch (Exception e) {
            log.error("Failed to save user recommendations to MongoDB: {}", e.getMessage(), e);
            return false;
        }
    }

    private int executeBulkOperation(MongoCollection<Document> collection, List<WriteModel<Document>> operations) {
        try {
            BulkWriteOptions options = new BulkWriteOptions().ordered(false);
            return collection.bulkWrite(operations, options).getModifiedCount();
        } catch (Exception e) {
            log.error("Error executing bulk operation: {}", e.getMessage(), e);
            return 0;
        }
    }

    /**
     * Converts a UserRecommendations object to a MongoDB Document.
     */
    private Document convertToDocument(UserRecommendations userRecs) {
        if (userRecs == null) {
            throw new IllegalArgumentException("UserRecommendations cannot be null");
        }

        Document document = new Document();
        document.append("userId", userRecs.getUserId());
        document.append("generatedAt", Date.from(userRecs.getGeneratedAt()));
        document.append("modelVersion", userRecs.getModelVersion());

        List<Document> recommendationDocs = new ArrayList<>();
        if (userRecs.getRecommendations() != null) {
            for (MovieRecommendation rec : userRecs.getRecommendations()) {
                if (rec != null) {  // Additional null check for individual recommendations
                    Document recDoc = new Document()
                            .append("movieId", rec.getMovieId())
                            .append("rating", rec.getRating())
                            .append("generatedAt", Date.from(rec.getGeneratedAt()));

                    // Add movie metadata if available
                    if (rec.getMovieTitle() != null) {
                        recDoc.append("movieTitle", rec.getMovieTitle());
                    }
                    if (rec.getMovieGenres() != null && !rec.getMovieGenres().isEmpty()) {
                        recDoc.append("movieGenres", rec.getMovieGenres());
                    }

                    recommendationDocs.add(recDoc);
                }
            }
        }

        document.append("recommendations", recommendationDocs);
        return document;
    }

    public void close() {
        if (mongoClient != null) {
            try {
                mongoClient.close();
                log.info("MongoDB connection closed");
            } catch (Exception e) {
                log.error("Error closing MongoDB connection: {}", e.getMessage(), e);
            }
        }
    }
}
