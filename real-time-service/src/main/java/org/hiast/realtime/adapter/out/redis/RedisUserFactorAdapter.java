
package org.hiast.realtime.adapter.out.redis;

import org.hiast.ids.UserId;
import org.hiast.model.factors.UserFactor;
import org.hiast.realtime.application.port.out.UserFactorPort;
import org.hiast.util.VectorSerializationUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.UnifiedJedis;

import java.util.Optional;

/**
 * Adapter implementation for fetching user factors from Redis using UnifiedJedis.
 */
public class RedisUserFactorAdapter implements UserFactorPort {

    private static final Logger LOG = LoggerFactory.getLogger(RedisUserFactorAdapter.class);
    private final UnifiedJedis jedis;

    public RedisUserFactorAdapter(UnifiedJedis jedis) {
        this.jedis = jedis;
    }

    @Override
    public Optional<UserFactor<float[]>> findUserFactorById(UserId userId) {
        final String KEY_PREFIX = "vector:user:";
        final String VECTOR_FIELD = "vector";


        String redisKey = KEY_PREFIX + userId.getUserId();

        try {

            byte[] userVectorBytes = jedis.hget(redisKey.getBytes(), VECTOR_FIELD.getBytes());

            if (userVectorBytes == null) {
                LOG.warn("Could not find vector in HASH field '{}' for key: {}. Please verify the key and field name.", VECTOR_FIELD, redisKey);
                return Optional.empty();
            }

            float[] vector = VectorSerializationUtil.deserializeVector(userVectorBytes);
            return Optional.of(new UserFactor<>(userId, vector));
        } catch (Exception e) {
            LOG.error("Failed to retrieve user factor for user: {} from key: {}", userId.getUserId(), redisKey, e);
            return Optional.empty();
        }
    }
}