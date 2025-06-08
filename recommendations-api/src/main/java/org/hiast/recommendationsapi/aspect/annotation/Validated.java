package org.hiast.recommendationsapi.aspect.annotation;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Annotation to mark methods for automatic validation.
 * Methods annotated with @Validated will have their parameters
 * validated before execution.
 */
@Target(ElementType.METHOD)
@Retention(RetentionPolicy.RUNTIME)
public @interface Validated {
    
    /**
     * Whether to validate null parameters.
     */
    boolean validateNulls() default true;
    
    /**
     * Whether to validate business rules (e.g., positive IDs).
     */
    boolean validateBusinessRules() default true;
    
    /**
     * Custom validation message.
     */
    String message() default "Validation failed";
}
