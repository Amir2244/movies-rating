package org.hiast.realtime.adapter.out.redis;

import org.hiast.ids.MovieId;
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
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Adapter implementation for performing vector similarity search in Redis.
 * It uses the FT.SEARCH command with a KNN query.
 */
public class RedisVectorSearchAdapter implements VectorSearchPort {
    private static final Logger LOG = LoggerFactory.getLogger(RedisVectorSearchAdapter.class);
    private final UnifiedJedis jedis;
    // FIX: Corrected the index name to match the redis-init script
    private static final String ITEM_INDEX_NAME = "item_factors_idx";

    public RedisVectorSearchAdapter(UnifiedJedis jedis) {
        this.jedis = jedis;
    }

    @Override
    public List<MovieRecommendation> findSimilarItems(UserFactor<float[]> userFactor, int topN, double eventWeight) {
        // Construct a KNN query for Redis Search
        // Use lowercase 'blob' for both the query string and parameter name
        String queryString = String.format("*=>[KNN %d @%s $blob AS score]", topN, VectorSerializationUtil.VECTOR_FIELD);
        byte[] vectorBytes = VectorSerializationUtil.serializeVector(userFactor.getFeatures());

        // Debug logging for the query and vector
        LOG.debug("KNN Query String: {}", queryString);
        LOG.debug("Serialized vector ({} bytes): {}", vectorBytes.length, Arrays.toString(Arrays.copyOf(vectorBytes, Math.min(16, vectorBytes.length))) + (vectorBytes.length > 16 ? "..." : ""));
        LOG.info("Using event weight: {} for rating calculation", eventWeight);

        Query query = new Query(queryString)
                .addParam("blob", vectorBytes)
                .returnFields("score")
                .setSortBy("score", true) // true for ascending (closer is smaller distance)
                .dialect(2);

        SearchResult searchResult = jedis.ftSearch(ITEM_INDEX_NAME, query);
        LOG.info("Search result: {}", searchResult);
        return searchResult.getDocuments().stream()
                .map(doc -> {
                    // The document ID is the key from Redis, e.g., "vector:item:456"
                    String[] idParts = doc.getId().split(":");
                    String movieIdStr = idParts.length > 2 ? idParts[2] : idParts[idParts.length - 1];
                    int movieId = Integer.parseInt(movieIdStr);
                    float closeness = Float.parseFloat(doc.getString("score"));
                    float score = 1 - closeness;

                    // Apply event weight to the score calculation
                    // Normalize the event weight to ensure it has a reasonable impact
                    // Higher event weights result in higher ratings
                    float normalizedWeight = (float) Math.min(2.0, Math.max(0.5, eventWeight / 2.5));
                    float weightedScore = score * normalizedWeight;

                    // Normalize to a 5-point scale, ensuring we don't exceed 5.0
                    float prediction = Math.min(5.0f, weightedScore * 5);

                    LOG.debug("Movie ID: {}, Base Score: {}, Weighted Score: {}, Final Prediction: {}", 
                             movieId, score, weightedScore, prediction);

                    return new MovieRecommendation(userFactor.getId(), MovieId.of(movieId), prediction, Instant.now());
                })
                .collect(Collectors.toList());
    }
}
