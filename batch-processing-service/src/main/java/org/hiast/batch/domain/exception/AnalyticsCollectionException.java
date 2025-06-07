package org.hiast.batch.domain.exception;

/**
 * Exception thrown when analytics collection fails.
 * This is a domain exception that represents business logic failures
 * in the analytics collection process.
 */
public class AnalyticsCollectionException extends RuntimeException {
    
    private final String analyticsType;
    
    public AnalyticsCollectionException(String analyticsType, String message) {
        super(String.format("Failed to collect %s analytics: %s", analyticsType, message));
        this.analyticsType = analyticsType;
    }
    
    public AnalyticsCollectionException(String analyticsType, String message, Throwable cause) {
        super(String.format("Failed to collect %s analytics: %s", analyticsType, message), cause);
        this.analyticsType = analyticsType;
    }
    
    public String getAnalyticsType() {
        return analyticsType;
    }
}
