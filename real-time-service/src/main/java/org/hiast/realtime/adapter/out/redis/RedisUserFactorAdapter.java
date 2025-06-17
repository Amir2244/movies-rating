
package org.hiast.realtime.adapter.out.redis;

import org.hiast.ids.UserId;
import org.hiast.model.factors.UserFactor;
import org.hiast.realtime.application.port.out.UserFactorPort;
import org.hiast.util.VectorSerializationUtil;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;

import java.util.Optional;

/**
 * Adapter implementation for fetching user factors from Redis.
 */
public class RedisUserFactorAdapter implements UserFactorPort {

    private final JedisPool jedisPool;

    public RedisUserFactorAdapter(JedisPool jedisPool) {
        this.jedisPool = jedisPool;
    }

    @Override
    public Optional<UserFactor> findUserFactorById(UserId userId) {
        try (Jedis jedis = jedisPool.getResource()) {
            byte[] userVectorBytes = jedis.hget("user_factors".getBytes(), String.valueOf(userId.getUserId()).getBytes());
            if (userVectorBytes == null) {
                return Optional.empty();
            }
            float[] vector = VectorSerializationUtil.deserializeVector(userVectorBytes);
            return Optional.of(new UserFactor(userId, vector));
        } catch (Exception e) {
            // Log exception
            return Optional.empty();
        }
    }
}