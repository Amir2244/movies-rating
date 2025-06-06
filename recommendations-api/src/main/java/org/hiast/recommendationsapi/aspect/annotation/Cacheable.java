package org.hiast.recommendationsapi.aspect.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;
import java.util.concurrent.TimeUnit;

/**
 * Custom caching annotation with enhanced features.
 * Extends Spring's @Cacheable with additional monitoring and fallback capabilities.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Cacheable {
    
    /**
     * Cache name(s) to use.
     */
    String[] value() default {};
    
    /**
     * Cache name(s) to use (alias for value).
     */
    String[] cacheNames() default {};
    
    /**
     * Spring Expression Language (SpEL) expression for computing the key dynamically.
     */
    String key() default "";
    
    /**
     * Spring Expression Language (SpEL) expression used for making the method
     * caching conditional.
     */
    String condition() default "";
    
    /**
     * Spring Expression Language (SpEL) expression used to veto method caching.
     */
    String unless() default "";
    
    /**
     * Time to live for cache entries.
     */
    long ttl() default -1;
    
    /**
     * Time unit for TTL.
     */
    TimeUnit timeUnit() default TimeUnit.SECONDS;
    
    /**
     * Whether to use local cache as fallback if distributed cache fails.
     */
    boolean useLocalFallback() default true;
    
    /**
     * Whether to monitor cache hit/miss metrics.
     */
    boolean monitorMetrics() default true;
}
