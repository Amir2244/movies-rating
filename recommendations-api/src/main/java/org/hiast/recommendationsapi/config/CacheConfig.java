package org.hiast.recommendationsapi.config;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.data.redis.cache.RedisCacheConfiguration;
import org.springframework.data.redis.cache.RedisCacheManager;
import org.springframework.data.redis.connection.RedisConnectionFactory;
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer;
import org.springframework.data.redis.serializer.RedisSerializationContext;
import org.springframework.data.redis.serializer.StringRedisSerializer;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Configuration for caching mechanisms.
 * Supports both Redis (distributed) and Caffeine (local) caching.
 */
@Configuration
@EnableCaching
public class CacheConfig {
    
    public static final String USER_RECOMMENDATIONS_CACHE = "userRecommendations";
    public static final String USER_RECOMMENDATIONS_LIMITED_CACHE = "userRecommendationsLimited";
    public static final String USER_EXISTS_CACHE = "userExists";
    
    @Value("${spring.cache.redis.time-to-live:3600000}")
    private long redisTtlMs;
    
    @Value("${app.cache.local.maximum-size:1000}")
    private long localCacheMaxSize;
    
    @Value("${app.cache.local.expire-after-write:PT30M}")
    private Duration localCacheExpireAfterWrite;
    
    /**
     * Primary cache manager using Redis for distributed caching.
     */
    @Bean
    @Primary
    public CacheManager redisCacheManager(RedisConnectionFactory redisConnectionFactory) {
        RedisCacheConfiguration config = RedisCacheConfiguration.defaultCacheConfig()
                .entryTtl(Duration.ofMillis(redisTtlMs))
                .serializeKeysWith(RedisSerializationContext.SerializationPair
                        .fromSerializer(new StringRedisSerializer()))
                .serializeValuesWith(RedisSerializationContext.SerializationPair
                        .fromSerializer(new GenericJackson2JsonRedisSerializer()))
                .disableCachingNullValues();
        
        return RedisCacheManager.builder(redisConnectionFactory)
                .cacheDefaults(config)
                .transactionAware()
                .build();
    }
    
    /**
     * Local cache manager using Caffeine for fast local caching.
     * Used as a fallback or for frequently accessed data.
     */
    @Bean("localCacheManager")
    @ConditionalOnProperty(name = "app.cache.local.enabled", havingValue = "true", matchIfMissing = true)
    public CacheManager caffeineCacheManager() {
        CaffeineCacheManager cacheManager = new CaffeineCacheManager();
        cacheManager.setCaffeine(Caffeine.newBuilder()
                .maximumSize(localCacheMaxSize)
                .expireAfterWrite(localCacheExpireAfterWrite.toMinutes(), TimeUnit.MINUTES)
                .recordStats());
        
        return cacheManager;
    }
    
    /**
     * Cache configuration for user recommendations with custom TTL.
     */
    @Bean
    public RedisCacheConfiguration userRecommendationsCacheConfig() {
        return RedisCacheConfiguration.defaultCacheConfig()
                .entryTtl(Duration.ofHours(2)) // Longer TTL for recommendations
                .serializeKeysWith(RedisSerializationContext.SerializationPair
                        .fromSerializer(new StringRedisSerializer()))
                .serializeValuesWith(RedisSerializationContext.SerializationPair
                        .fromSerializer(new GenericJackson2JsonRedisSerializer()))
                .disableCachingNullValues();
    }
    
    /**
     * Cache configuration for user existence checks with shorter TTL.
     */
    @Bean
    public RedisCacheConfiguration userExistsCacheConfig() {
        return RedisCacheConfiguration.defaultCacheConfig()
                .entryTtl(Duration.ofMinutes(15)) // Shorter TTL for existence checks
                .serializeKeysWith(RedisSerializationContext.SerializationPair
                        .fromSerializer(new StringRedisSerializer()))
                .serializeValuesWith(RedisSerializationContext.SerializationPair
                        .fromSerializer(new GenericJackson2JsonRedisSerializer()))
                .disableCachingNullValues();
    }
}
