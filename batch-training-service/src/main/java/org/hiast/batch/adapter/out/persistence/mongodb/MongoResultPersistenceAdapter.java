package org.hiast.batch.adapter.out.persistence.mongodb;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.ConnectionString;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
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

/**
 * Adapter implementing ResultPersistencePort to save recommendation results to MongoDB.
 */
public class MongoResultPersistenceAdapter implements ResultPersistencePort {
    private static final Logger log = LoggerFactory.getLogger(MongoResultPersistenceAdapter.class);

    private final MongoConfig mongoConfig;

    public MongoResultPersistenceAdapter(MongoConfig mongoConfig) {
        this.mongoConfig = mongoConfig;
        log.info("MongoResultPersistenceAdapter initialized with config: {}", mongoConfig);
    }

    @Override
    public boolean saveUserRecommendations(List<UserRecommendations> userRecommendationsList) {
        if (userRecommendationsList == null || userRecommendationsList.isEmpty()) {
            log.warn("No user recommendations to save. Skipping MongoDB persistence.");
            return false;
        }

        log.info("Saving {} user recommendations to MongoDB...", userRecommendationsList.size());
        String uri = mongoConfig.getConnectionString();
        try (MongoClient mongoClient = MongoClients.create(uri)) {
            MongoDatabase database = mongoClient.getDatabase(mongoConfig.getDatabase());
            MongoCollection<Document> collection = database.getCollection(mongoConfig.getRecommendationsCollection());

            int successCount = 0;
            for (UserRecommendations userRecs : userRecommendationsList) {
                try {
                    Document document = convertToDocument(userRecs);

                    // Use upsert to replace existing recommendations for the same user
                    ReplaceOptions options = new ReplaceOptions().upsert(true);
                    collection.replaceOne(
                            Filters.eq("userId", userRecs.getUserId()),
                            document,
                            options
                    );

                    successCount++;
                } catch (Exception e) {
                    log.error("Error saving recommendations for user {}: {}", userRecs.getUserId(), e.getMessage(), e);
                }
            }

            log.info("Successfully saved {}/{} user recommendations to MongoDB",
                    successCount, userRecommendationsList.size());

            return successCount > 0;
        } catch (Exception e) {
            log.error("Failed to save user recommendations to MongoDB: {}", e.getMessage(), e);
            return false;
        }
    }

    /**
     * Converts a UserRecommendations object to a MongoDB Document.
     */
    private Document convertToDocument(UserRecommendations userRecs) {
        Document document = new Document();
        document.append("userId", userRecs.getUserId());
        document.append("generatedAt", Date.from(userRecs.getGeneratedAt()));
        document.append("modelVersion", userRecs.getModelVersion());

        List<Document> recommendationDocs = new ArrayList<>();
        for (MovieRecommendation rec : userRecs.getRecommendations()) {
            Document recDoc = new Document()
                    .append("movieId", rec.getMovieId())
                    .append("rating", rec.getRating())
                    .append("generatedAt", Date.from(rec.getGeneratedAt()));
            recommendationDocs.add(recDoc);
        }

        document.append("recommendations", recommendationDocs);
        return document;
    }
}
