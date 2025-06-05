package org.hiast.recommendationsapi.aspect;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.hiast.recommendationsapi.aspect.annotation.Monitored;
import org.hiast.recommendationsapi.config.AopConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * Aspect for monitoring method performance and execution metrics.
 * Tracks execution time, logs slow operations, and collects metrics.
 */
@Aspect
@Component
@ConditionalOnProperty(name = "app.monitoring.performance.enabled", havingValue = "true", matchIfMissing = true)
public class PerformanceMonitoringAspect {
    
    private static final Logger log = LoggerFactory.getLogger(PerformanceMonitoringAspect.class);
    
    private final AopConfig aopConfig;
    private final MeterRegistry meterRegistry;
    
    public PerformanceMonitoringAspect(AopConfig aopConfig, MeterRegistry meterRegistry) {
        this.aopConfig = aopConfig;
        this.meterRegistry = meterRegistry;
    }
    
    /**
     * Around advice for methods annotated with @Monitored.
     */
    @Around("@annotation(monitored)")
    public Object monitorPerformance(ProceedingJoinPoint joinPoint, Monitored monitored) throws Throwable {
        String operationName = getOperationName(joinPoint, monitored);
        long startTime = System.nanoTime();
        
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try {
            if (monitored.logParameters()) {
                log.debug("Executing {} with parameters: {}", operationName, Arrays.toString(joinPoint.getArgs()));
            } else {
                log.debug("Executing {}", operationName);
            }
            
            Object result = joinPoint.proceed();
            
            long executionTime = System.nanoTime() - startTime;
            long executionTimeMs = TimeUnit.NANOSECONDS.toMillis(executionTime);
            
            // Record metrics
            sample.stop(Timer.builder("method.execution.time")
                    .description("Method execution time")
                    .tag("class", joinPoint.getTarget().getClass().getSimpleName())
                    .tag("method", joinPoint.getSignature().getName())
                    .tag("operation", operationName)
                    .register(meterRegistry));
            
            // Check if execution was slow
            long threshold = monitored.thresholdMs() > 0 ? 
                monitored.thresholdMs() : aopConfig.getPerformance().getSlowThresholdMs();
            
            if (executionTimeMs > threshold) {
                log.warn("Slow operation detected: {} took {}ms (threshold: {}ms)", 
                    operationName, executionTimeMs, threshold);
                
                // Record slow operation metric
                meterRegistry.counter("method.execution.slow",
                        "class", joinPoint.getTarget().getClass().getSimpleName(),
                        "method", joinPoint.getSignature().getName(),
                        "operation", operationName)
                        .increment();
            } else {
                log.debug("Completed {} in {}ms", operationName, executionTimeMs);
            }
            
            if (monitored.logReturnValue() && result != null) {
                log.debug("Method {} returned: {}", operationName, result.toString());
            }
            
            return result;
            
        } catch (Exception e) {
            long executionTime = System.nanoTime() - startTime;
            long executionTimeMs = TimeUnit.NANOSECONDS.toMillis(executionTime);
            
            log.error("Error in {} after {}ms: {}", operationName, executionTimeMs, e.getMessage(), e);
            
            // Record error metric
            meterRegistry.counter("method.execution.error",
                    "class", joinPoint.getTarget().getClass().getSimpleName(),
                    "method", joinPoint.getSignature().getName(),
                    "operation", operationName,
                    "exception", e.getClass().getSimpleName())
                    .increment();
            
            throw e;
        }
    }
    
    /**
     * Around advice for all service methods (fallback monitoring).
     */
    @Around("execution(* org.hiast.recommendationsapi.application.service.*.*(..))")
    public Object monitorServiceMethods(ProceedingJoinPoint joinPoint) throws Throwable {
        // Only monitor if method is not already annotated with @Monitored
        Method method = ((MethodSignature) joinPoint.getSignature()).getMethod();
        if (method.isAnnotationPresent(Monitored.class)) {
            return joinPoint.proceed();
        }
        
        String operationName = joinPoint.getTarget().getClass().getSimpleName() + "." + 
                              joinPoint.getSignature().getName();
        long startTime = System.nanoTime();
        
        Timer.Sample sample = Timer.start(meterRegistry);
        
        try {
            log.debug("Executing service method: {}", operationName);
            
            Object result = joinPoint.proceed();
            
            long executionTime = System.nanoTime() - startTime;
            long executionTimeMs = TimeUnit.NANOSECONDS.toMillis(executionTime);
            
            sample.stop(Timer.builder("service.method.execution.time")
                    .description("Service method execution time")
                    .tag("class", joinPoint.getTarget().getClass().getSimpleName())
                    .tag("method", joinPoint.getSignature().getName())
                    .register(meterRegistry));
            
            log.debug("Completed service method {} in {}ms", operationName, executionTimeMs);
            
            return result;
            
        } catch (Exception e) {
            log.error("Error in service method {}: {}", operationName, e.getMessage(), e);
            
            meterRegistry.counter("service.method.error",
                    "class", joinPoint.getTarget().getClass().getSimpleName(),
                    "method", joinPoint.getSignature().getName(),
                    "exception", e.getClass().getSimpleName())
                    .increment();
            
            throw e;
        }
    }
    
    private String getOperationName(ProceedingJoinPoint joinPoint, Monitored monitored) {
        if (!monitored.value().isEmpty()) {
            return monitored.value();
        }
        return joinPoint.getTarget().getClass().getSimpleName() + "." + joinPoint.getSignature().getName();
    }
}
