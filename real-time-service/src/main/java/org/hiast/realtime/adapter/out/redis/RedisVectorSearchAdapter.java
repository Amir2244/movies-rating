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
    public List<MovieRecommendation> findSimilarItems(UserFactor<float[]> userFactor, int topN) {
        // Construct a KNN query for Redis Search
        // Use lowercase 'blob' for both the query string and parameter name
        String queryString = String.format("*=>[KNN %d @%s $blob AS score]", topN, VectorSerializationUtil.VECTOR_FIELD);
        byte[] vectorBytes = VectorSerializationUtil.serializeVector(userFactor.getFeatures());

        // Debug logging for the query and vector
        LOG.debug("KNN Query String: {}", queryString);
        LOG.debug("Serialized vector ({} bytes): {}", vectorBytes.length, Arrays.toString(Arrays.copyOf(vectorBytes, Math.min(16, vectorBytes.length))) + (vectorBytes.length > 16 ? "..." : ""));

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
                    float similarityScore = Float.parseFloat(doc.getString("score"));
                    return new MovieRecommendation(userFactor.getId(), MovieId.of(movieId), similarityScore, Instant.now());
                })
                .collect(Collectors.toList());
    }
}
