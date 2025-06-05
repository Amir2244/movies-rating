package org.hiast.recommendationsapi.aspect;

import io.micrometer.core.instrument.MeterRegistry;
import org.aspectj.lang.ProceedingJoinPoint;
import org.aspectj.lang.annotation.Around;
import org.aspectj.lang.annotation.Aspect;
import org.aspectj.lang.reflect.MethodSignature;
import org.hiast.ids.UserId;
import org.hiast.recommendationsapi.aspect.annotation.Validated;
import org.hiast.recommendationsapi.domain.exception.InvalidRecommendationRequestException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.util.Collection;

/**
 * Aspect for handling automatic validation of method parameters.
 * Validates null checks, business rules, and other constraints.
 */
@Aspect
@Component
public class ValidationAspect {
    
    private static final Logger log = LoggerFactory.getLogger(ValidationAspect.class);
    
    private final MeterRegistry meterRegistry;
    
    public ValidationAspect(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }
    
    @Around("@annotation(validated)")
    public Object handleValidation(ProceedingJoinPoint joinPoint, Validated validated) throws Throwable {
        Method method = ((MethodSignature) joinPoint.getSignature()).getMethod();
        String methodName = joinPoint.getTarget().getClass().getSimpleName() + "." + method.getName();
        Object[] args = joinPoint.getArgs();
        Parameter[] parameters = method.getParameters();
        
        log.debug("Validating method parameters for: {}", methodName);
        
        try {
            // Validate null parameters
            if (validated.validateNulls()) {
                validateNullParameters(args, parameters, methodName);
            }
            
            // Validate business rules
            if (validated.validateBusinessRules()) {
                validateBusinessRules(args, parameters, methodName);
            }
            
            // Record successful validation
            recordValidationResult(methodName, "success");
            
            return joinPoint.proceed();
            
        } catch (InvalidRecommendationRequestException e) {
            recordValidationResult(methodName, "failed");
            log.warn("Validation failed for method {}: {}", methodName, e.getMessage());
            throw e;
        } catch (Exception e) {
            recordValidationResult(methodName, "error");
            log.error("Validation error for method {}: {}", methodName, e.getMessage(), e);
            throw new InvalidRecommendationRequestException(
                validated.message().isEmpty() ? "Validation failed" : validated.message(), e);
        }
    }
    
    private void validateNullParameters(Object[] args, Parameter[] parameters, String methodName) {
        for (int i = 0; i < args.length; i++) {
            if (args[i] == null) {
                String paramName = parameters[i].getName();
                String message = String.format("Parameter '%s' cannot be null in method %s", paramName, methodName);
                throw new InvalidRecommendationRequestException(message);
            }
        }
    }
    
    private void validateBusinessRules(Object[] args, Parameter[] parameters, String methodName) {
        for (int i = 0; i < args.length; i++) {
            Object arg = args[i];
            if (arg == null) continue; // Already validated in null check
            
            Parameter parameter = parameters[i];
            validateSingleParameter(arg, parameter, methodName);
        }
    }
    
    private void validateSingleParameter(Object arg, Parameter parameter, String methodName) {
        Class<?> paramType = parameter.getType();
        String paramName = parameter.getName();
        
        // Validate UserId
        if (paramType == UserId.class) {
            validateUserId((UserId) arg, paramName, methodName);
        }
        
        // Validate Integer (commonly used for limits, counts, etc.)
        else if (paramType == Integer.class || paramType == int.class) {
            validateIntegerParameter((Integer) arg, paramName, methodName);
        }
        
        // Validate Collections
        else if (Collection.class.isAssignableFrom(paramType)) {
            validateCollectionParameter((Collection<?>) arg, paramName, methodName);
        }
        
        // Validate Strings
        else if (paramType == String.class) {
            validateStringParameter((String) arg, paramName, methodName);
        }
        
        // Add more validation rules as needed
    }
    
    private void validateUserId(UserId userId, String paramName, String methodName) {
        if (userId.getUserId() <= 0) {
            throw new InvalidRecommendationRequestException(
                String.format("User ID must be positive in parameter '%s' for method %s, but was: %d", 
                    paramName, methodName, userId.getUserId()));
        }
    }
    
    private void validateIntegerParameter(Integer value, String paramName, String methodName) {
        // Common validations for integer parameters
        if (paramName.toLowerCase().contains("limit")) {
            if (value <= 0) {
                throw new InvalidRecommendationRequestException(
                    String.format("Limit parameter '%s' must be positive in method %s, but was: %d", 
                        paramName, methodName, value));
            }
            if (value > 100) { // Reasonable upper limit
                throw new InvalidRecommendationRequestException(
                    String.format("Limit parameter '%s' exceeds maximum allowed (100) in method %s, but was: %d", 
                        paramName, methodName, value));
            }
        }
        
        if (paramName.toLowerCase().contains("id") && value <= 0) {
            throw new InvalidRecommendationRequestException(
                String.format("ID parameter '%s' must be positive in method %s, but was: %d", 
                    paramName, methodName, value));
        }
    }
    
    private void validateCollectionParameter(Collection<?> collection, String paramName, String methodName) {
        if (collection.isEmpty()) {
            throw new InvalidRecommendationRequestException(
                String.format("Collection parameter '%s' cannot be empty in method %s", paramName, methodName));
        }
        
        // Check for reasonable size limits
        if (collection.size() > 100) {
            throw new InvalidRecommendationRequestException(
                String.format("Collection parameter '%s' exceeds maximum size (100) in method %s, but was: %d", 
                    paramName, methodName, collection.size()));
        }
    }
    
    private void validateStringParameter(String value, String paramName, String methodName) {
        if (value.trim().isEmpty()) {
            throw new InvalidRecommendationRequestException(
                String.format("String parameter '%s' cannot be empty in method %s", paramName, methodName));
        }
        
        // Add length validations if needed
        if (value.length() > 255) {
            throw new InvalidRecommendationRequestException(
                String.format("String parameter '%s' exceeds maximum length (255) in method %s", 
                    paramName, methodName));
        }
    }
    
    private void recordValidationResult(String methodName, String result) {
        if (meterRegistry != null) {
            meterRegistry.counter("validation.requests",
                    "method", methodName,
                    "result", result)
                    .increment();
        }
    }
} 