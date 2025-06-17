

package org.hiast.realtime.adapter.out.redis;

import org.hiast.ids.MovieId;
import org.hiast.model.MovieRecommendation;
import org.hiast.model.factors.UserFactor;
import org.hiast.realtime.application.port.out.VectorSearchPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.search.Query;
import redis.clients.jedis.search.SearchResult;

import java.time.Instant;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Adapter implementation for performing vector similarity search in Redis.
 * It uses the FT.SEARCH command with a KNN query.
 */
public class RedisVectorSearchAdapter implements VectorSearchPort {

    private final JedisPool jedisPool;
    private static final String ITEM_INDEX_NAME = "item_vectors_idx";

    public RedisVectorSearchAdapter(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    @Override
    public List<MovieRecommendation> findSimilarItems(UserFactor userFactor, int topN) {
        try (Jedis jedis = jedisPool.getResource()) {
            // Construct a KNN query for Redis Search
            // This query finds the 'topN' nearest neighbors to the user's vector
            // based on cosine similarity (L2 distance is also common).
            String queryString = String.format("*=>[KNN %d @vector $blob AS a]", topN);
            Query query = new Query(queryString)
                    .addParam("blob", userFactor.getFeatures())
                    .returnFields("a")
                    .setSortBy("a", true) // true for ascending (closer is smaller distance)
                    .dialect(2);

            SearchResult searchResult = jedis.ftSearch(ITEM_INDEX_NAME, query);

            return searchResult.getDocuments().stream()
                    .map(doc -> {
                        String movieIdStr = doc.getId().split(":")[1];
                        int movieId = Integer.parseInt(movieIdStr);
                        double score = doc.getScore();
                        float floatScore = Float.parseFloat(String.valueOf(score));
                        return new MovieRecommendation(userFactor.getId(),MovieId.of(movieId), floatScore,Instant.now());
                    })
                    .collect(Collectors.toList());
        }
    }

}