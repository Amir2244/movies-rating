package org.hiast.realtime.adapter.out.redis;

import org.hiast.ids.MovieId;
import org.hiast.ids.UserId;
import org.hiast.model.MovieRecommendation;
import org.hiast.model.factors.UserFactor;
import org.hiast.realtime.application.port.out.VectorSearchPort;
import org.hiast.util.VectorSerializationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.UnifiedJedis;
import redis.clients.jedis.search.Query;
import redis.clients.jedis.search.SearchResult;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

/**
 * Adapter implementation for performing vector similarity search in Redis.
 * It uses the FT.SEARCH command with a KNN query.
 */
public class RedisVectorSearchAdapter implements VectorSearchPort {
    private static final Logger LOG = LoggerFactory.getLogger(RedisVectorSearchAdapter.class);
    private final UnifiedJedis jedis;
    private static final String ITEM_INDEX_NAME = "item_factors_idx";

    public RedisVectorSearchAdapter(UnifiedJedis jedis) {
        this.jedis = jedis;
    }

    @Override
    public List<MovieRecommendation> findSimilarItems(UserFactor<float[]> userFactor, int topN, double eventWeight) {
        byte[] vectorBytes = VectorSerializationUtil.serializeVector(userFactor.getFeatures());
        LOG.debug("Serialized vector ({} bytes): {}", vectorBytes.length, Arrays.toString(Arrays.copyOf(vectorBytes, Math.min(16, vectorBytes.length))) + (vectorBytes.length > 16 ? "..." : ""));

        SearchResult searchResult = performVectorSearch(vectorBytes, topN, eventWeight);
        LOG.info("Search result: {}", searchResult);

        return processSearchResults(searchResult, userFactor.getId(), eventWeight, null);
    }

    @Override
    public List<MovieRecommendation> findSimilarItemsByMovie(UserFactor<float[]> userFactor, MovieId movieId, int topN, double eventWeight) {
        SearchResult searchResult;

        try {
            String movieKey = VectorSerializationUtil.createItemFactorKey(movieId.getMovieId());
            LOG.info("Retrieving movie vector for key: {}", movieKey);
            if (!jedis.exists(movieKey)) {
                LOG.warn("Movie not found in Redis for movie ID: {}", movieId.getMovieId());
                return Collections.emptyList();
            }
            byte[] vectorField = jedis.hget(movieKey.getBytes(), VectorSerializationUtil.VECTOR_FIELD.getBytes());
            if (vectorField == null) {
                LOG.warn("Vector field not found for movie ID: {}", movieId.getMovieId());
                return Collections.emptyList();
            }
            float[] movieVector = VectorSerializationUtil.deserializeVector(vectorField);
            LOG.debug("Retrieved and deserialized movie vector (dimension: {}) for movie ID: {}", 
                    movieVector.length, movieId.getMovieId());
            byte[] movieVectorBytes = VectorSerializationUtil.serializeVector(movieVector);
            LOG.debug("Using movie vector (dimension: {}) for search", movieVector.length);

            searchResult = performVectorSearch(movieVectorBytes, topN, eventWeight);
            LOG.info("Search result Using Similarity between Rated movie and others: {}", searchResult);
        } catch (Exception e) {
            LOG.error("Error performing vector search for movie ID: {}", movieId.getMovieId(), e);
            return Collections.emptyList();
        }

        return processSearchResults(searchResult, userFactor.getId(), eventWeight, movieId);
    }

    /**
     * Creates and executes a KNN query for vector search
     */
    private SearchResult performVectorSearch(byte[] vectorBytes, int topN, double eventWeight) {
        // Construct a KNN query for Redis Search
        String queryString = String.format("*=>[KNN %d @%s $blob AS score]", topN, VectorSerializationUtil.VECTOR_FIELD);
        LOG.debug("KNN Query String: {}", queryString);
        LOG.info("Using event weight: {} for rating calculation", eventWeight);

        Query query = new Query(queryString)
                .addParam("blob", vectorBytes)
                .returnFields("score")
                .setSortBy("score", true) // true for ascending (closer is smaller distance)
                .dialect(2);

        return jedis.ftSearch(ITEM_INDEX_NAME, query);
    }

    /**
     * Processes search results to create MovieRecommendation objects
     */
    private List<MovieRecommendation> processSearchResults(SearchResult searchResult, UserId userId, double eventWeight, MovieId originalMovieId) {
        return searchResult.getDocuments().stream()
                .map(doc -> {
                    try {
                        // The document ID is the key from Redis, e.g., "vector:item:456"
                        String[] idParts = doc.getId().split(":");
                        String movieIdStr = idParts.length > 2 ? idParts[2] : idParts[idParts.length - 1];
                        int movieId = Integer.parseInt(movieIdStr);

                        // Skip the original movie in the results if provided
                        if (originalMovieId != null && movieId == originalMovieId.getMovieId()) {
                            return null;
                        }

                        // Get score from document
                        float closeness = originalMovieId == null ? 
                            Float.parseFloat(doc.getString("score")) : 
                            Float.parseFloat(doc.get("score").toString());

                        // Calculate prediction
                        float prediction = calculatePrediction(closeness, eventWeight);

                        LOG.debug("Movie ID: {}, Prediction: {}", movieId, prediction);

                        return new MovieRecommendation(userId, MovieId.of(movieId), prediction, Instant.now());
                    } catch (Exception e) {
                        if (originalMovieId != null) {
                            LOG.error("Error processing search result document: {}", doc, e);
                        }
                        return null;
                    }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList());
    }

    /**
     * Calculates the prediction score based on closeness and event weight
     */
    private float calculatePrediction(float closeness, double eventWeight) {
        float score = 1 - closeness;

        // Apply event weight to the score calculation
        // Normalize the event weight to ensure it has a reasonable impact
        // Higher event weights result in higher ratings
        float normalizedWeight = (float) Math.min(2.0, Math.max(0.5, eventWeight / 2.5));
        float weightedScore = score * normalizedWeight;

        // Normalize to a 5-point scale, ensuring we don't exceed 5.0
        return Math.min(5.0f, weightedScore * 5);
    }
}
