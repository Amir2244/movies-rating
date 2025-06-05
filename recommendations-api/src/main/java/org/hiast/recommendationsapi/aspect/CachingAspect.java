package org.hiast.recommendationsapi.aspect;

import io.micrometer.core.instrument.MeterRegistry;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.hiast.recommendationsapi.aspect.annotation.Cacheable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.cache.Cache;
import org.springframework.cache.CacheManager;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Aspect for handling custom @Cacheable annotation with enhanced features.
 * Provides caching with local fallback, TTL support, and metrics monitoring.
 */
@Aspect
@Component
public class CachingAspect {
    
    private static final Logger log = LoggerFactory.getLogger(CachingAspect.class);
    
    private final CacheManager primaryCacheManager;
    private final CacheManager localCacheManager;
    private final MeterRegistry meterRegistry;
    
    public CachingAspect(@Qualifier("redisCacheManager") CacheManager primaryCacheManager, 
                        @Qualifier("localCacheManager") CacheManager localCacheManager,
                        MeterRegistry meterRegistry) {
        this.primaryCacheManager = primaryCacheManager;
        this.localCacheManager = localCacheManager;
        this.meterRegistry = meterRegistry;
    }
    
    @Around("@annotation(cacheable)")
    public Object handleCaching(ProceedingJoinPoint joinPoint, Cacheable cacheable) throws Throwable {
        Method method = ((MethodSignature) joinPoint.getSignature()).getMethod();
        String methodName = joinPoint.getTarget().getClass().getSimpleName() + "." + method.getName();
        
        // Determine cache names
        String[] cacheNames = cacheable.cacheNames().length > 0 ? 
            cacheable.cacheNames() : cacheable.value();
        
        if (cacheNames.length == 0) {
            log.warn("No cache names specified for method: {}", methodName);
            return joinPoint.proceed();
        }
        
        String primaryCacheName = cacheNames[0];
        String cacheKey = generateCacheKey(joinPoint, cacheable);
        
        // Check condition
        if (!evaluateCondition(cacheable.condition(), joinPoint)) {
            log.debug("Cache condition not met for method: {}", methodName);
            return joinPoint.proceed();
        }
        
        // Try to get from primary cache
        Object cachedResult = getFromPrimaryCache(primaryCacheName, cacheKey, methodName);
        if (cachedResult != null) {
            recordCacheHit(methodName, primaryCacheName, "primary");
            return cachedResult;
        }
        
        // Try local fallback if enabled
        if (cacheable.useLocalFallback()) {
            cachedResult = getFromLocalCache(primaryCacheName, cacheKey, methodName);
            if (cachedResult != null) {
                recordCacheHit(methodName, primaryCacheName, "local_fallback");
                return cachedResult;
            }
        }
        
        // Cache miss - execute method
        recordCacheMiss(methodName, primaryCacheName);
        Object result = joinPoint.proceed();
        
        // Check unless condition
        if (!evaluateUnlessCondition(cacheable.unless(), joinPoint, result)) {
            // Store in caches
            storeInPrimaryCache(primaryCacheName, cacheKey, result, cacheable, methodName);
            if (cacheable.useLocalFallback()) {
                storeInLocalCache(primaryCacheName, cacheKey, result, cacheable, methodName);
            }
        }
        
        return result;
    }
    
    private String generateCacheKey(ProceedingJoinPoint joinPoint, Cacheable cacheable) {
        if (!cacheable.key().isEmpty()) {
            // For now, simplified key generation. In production, you'd use SpEL evaluation
            return cacheable.key();
        }
        
        // Default key generation based on method parameters
        Object[] args = joinPoint.getArgs();
        if (args.length == 0) {
            return "no-args";
        }
        
        StringBuilder keyBuilder = new StringBuilder();
        for (Object arg : args) {
            if (arg != null) {
                keyBuilder.append(arg.toString()).append(":");
            } else {
                keyBuilder.append("null:");
            }
        }
        
        return keyBuilder.toString();
    }
    
    private boolean evaluateCondition(String condition, ProceedingJoinPoint joinPoint) {
        // Simplified condition evaluation. In production, you'd use SpEL
        if (condition.isEmpty()) {
            return true;
        }
        // For now, always return true. Implement SpEL evaluation as needed
        return true;
    }
    
    private boolean evaluateUnlessCondition(String unless, ProceedingJoinPoint joinPoint, Object result) {
        // Simplified unless evaluation. In production, you'd use SpEL
        if (unless.isEmpty()) {
            return false;
        }
        // For now, always return false. Implement SpEL evaluation as needed
        return false;
    }
    
    private Object getFromPrimaryCache(String cacheName, String key, String methodName) {
        try {
            Cache cache = primaryCacheManager.getCache(cacheName);
            if (cache != null) {
                Cache.ValueWrapper wrapper = cache.get(key);
                if (wrapper != null) {
                    log.debug("Cache hit in primary cache for method: {}, key: {}", methodName, key);
                    return wrapper.get();
                }
            }
        } catch (Exception e) {
            log.warn("Error accessing primary cache for method: {}, key: {}", methodName, key, e);
        }
        return null;
    }
    
    private Object getFromLocalCache(String cacheName, String key, String methodName) {
        try {
            Cache cache = localCacheManager.getCache(cacheName);
            if (cache != null) {
                Cache.ValueWrapper wrapper = cache.get(key);
                if (wrapper != null) {
                    log.debug("Cache hit in local fallback cache for method: {}, key: {}", methodName, key);
                    return wrapper.get();
                }
            }
        } catch (Exception e) {
            log.warn("Error accessing local cache for method: {}, key: {}", methodName, key, e);
        }
        return null;
    }
    
    private void storeInPrimaryCache(String cacheName, String key, Object value, 
                                   Cacheable cacheable, String methodName) {
        try {
            Cache cache = primaryCacheManager.getCache(cacheName);
            if (cache != null) {
                cache.put(key, value);
                log.debug("Stored in primary cache for method: {}, key: {}", methodName, key);
            }
        } catch (Exception e) {
            log.warn("Error storing in primary cache for method: {}, key: {}", methodName, key, e);
        }
    }
    
    private void storeInLocalCache(String cacheName, String key, Object value, 
                                 Cacheable cacheable, String methodName) {
        try {
            Cache cache = localCacheManager.getCache(cacheName);
            if (cache != null) {
                cache.put(key, value);
                log.debug("Stored in local cache for method: {}, key: {}", methodName, key);
            }
        } catch (Exception e) {
            log.warn("Error storing in local cache for method: {}, key: {}", methodName, key, e);
        }
    }
    
    private void recordCacheHit(String methodName, String cacheName, String cacheType) {
        if (meterRegistry != null) {
            meterRegistry.counter("cache.requests",
                    "method", methodName,
                    "cache", cacheName,
                    "type", cacheType,
                    "result", "hit")
                    .increment();
        }
    }
    
    private void recordCacheMiss(String methodName, String cacheName) {
        if (meterRegistry != null) {
            meterRegistry.counter("cache.requests",
                    "method", methodName,
                    "cache", cacheName,
                    "result", "miss")
                    .increment();
        }
    }
} 