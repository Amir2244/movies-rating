package org.hiast.recommendationsapi.config;

import com.github.benmanes.caffeine.cache.Caffeine;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cache.CacheManager;
import org.springframework.cache.annotation.EnableCaching;
import org.springframework.cache.caffeine.CaffeineCacheManager;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

/**
 * Configuration for caching using Caffeine (local in-memory cache).
 * Simple and fast local caching without external dependencies.
 */
@Configuration
@EnableCaching
public class CacheConfig {
    
    public static final String USER_RECOMMENDATIONS_CACHE = "userRecommendations";
    public static final String USER_RECOMMENDATIONS_LIMITED_CACHE = "userRecommendationsLimited";
    public static final String USER_EXISTS_CACHE = "userExists";
    
    @Value("${app.cache.local.maximum-size:1000}")
    private long localCacheMaxSize;
    
    @Value("${app.cache.local.expire-after-write:PT30M}")
    private Duration localCacheExpireAfterWrite;
    
    /**
     * Primary cache manager using Caffeine for local in-memory caching.
     * Fast, lightweight, and no external dependencies required.
     */
    @Bean
    @Primary
    public CacheManager caffeineCacheManager() {
        CaffeineCacheManager cacheManager = new CaffeineCacheManager();
        cacheManager.setCaffeine(Caffeine.newBuilder()
                .maximumSize(localCacheMaxSize)
                .expireAfterWrite(localCacheExpireAfterWrite.toMinutes(), TimeUnit.MINUTES)
                .recordStats());

        cacheManager.setCacheNames(java.util.Arrays.asList(
            USER_RECOMMENDATIONS_CACHE,
            USER_RECOMMENDATIONS_LIMITED_CACHE,
            USER_EXISTS_CACHE
        ));
        
        return cacheManager;
    }
}
