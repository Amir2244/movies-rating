package org.hiast.model.exception;

/**
 * Exception thrown when a domain object is invalid or violates business rules.
 * This can be used across all services for consistent validation error handling.
 */
public class InvalidDomainObjectException extends DomainException {

    private final String objectType;
    private final Object invalidValue;

    public InvalidDomainObjectException(String objectType, Object invalidValue, String message) {
        super(String.format("Invalid %s: %s - %s", objectType, invalidValue, message));
        this.objectType = objectType;
        this.invalidValue = invalidValue;
    }

    public InvalidDomainObjectException(String objectType, Object invalidValue, String message, Throwable cause) {
        super(String.format("Invalid %s: %s - %s", objectType, invalidValue, message), cause);
        this.objectType = objectType;
        this.invalidValue = invalidValue;
    }

    public String getObjectType() {
        return objectType;
    }

    public Object getInvalidValue() {
        return invalidValue;
    }
}