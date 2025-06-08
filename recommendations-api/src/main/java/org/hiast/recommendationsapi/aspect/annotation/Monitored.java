package org.hiast.recommendationsapi.aspect.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to mark methods for performance monitoring.
 * Methods annotated with @Monitored will have their execution time tracked
 * and logged if they exceed the configured threshold.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Monitored {
    
    /**
     * Custom name for the operation being monitored.
     * If not provided, the method name will be used.
     */
    String value() default "";
    
    /**
     * Whether to log method parameters (be careful with sensitive data).
     */
    boolean logParameters() default false;
    
    /**
     * Whether to log the return value (be careful with large objects).
     */
    boolean logReturnValue() default false;
    
    /**
     * Custom threshold in milliseconds for this specific method.
     * If not set, the global threshold will be used.
     */
    long thresholdMs() default -1;
}
