package org.hiast.recommendationsapi.adapter.in.web.exception;

import org.hiast.recommendationsapi.domain.exception.InvalidRecommendationRequestException;
import org.hiast.recommendationsapi.domain.exception.UserRecommendationsNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.context.request.WebRequest;

import java.time.Instant;
import java.util.HashMap;
import java.util.Map;

/**
 * Global exception handler for the recommendations API.
 * This provides centralized error handling and consistent error responses.
 */
@RestControllerAdvice
public class GlobalExceptionHandler {
    
    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);
    
    /**
     * Handles InvalidRecommendationRequestException.
     */
    @ExceptionHandler(InvalidRecommendationRequestException.class)
    public ResponseEntity<Map<String, Object>> handleInvalidRecommendationRequest(
            InvalidRecommendationRequestException ex, WebRequest request) {
        log.warn("Invalid recommendation request: {}", ex.getMessage());
        
        Map<String, Object> errorResponse = createErrorResponse(
                HttpStatus.BAD_REQUEST,
                "Invalid Request",
                ex.getMessage(),
                request.getDescription(false)
        );
        
        return ResponseEntity.badRequest().body(errorResponse);
    }
    
    /**
     * Handles UserRecommendationsNotFoundException.
     */
    @ExceptionHandler(UserRecommendationsNotFoundException.class)
    public ResponseEntity<Map<String, Object>> handleUserRecommendationsNotFound(
            UserRecommendationsNotFoundException ex, WebRequest request) {
        log.info("User recommendations not found: {}", ex.getMessage());
        
        Map<String, Object> errorResponse = createErrorResponse(
                HttpStatus.NOT_FOUND,
                "Recommendations Not Found",
                ex.getMessage(),
                request.getDescription(false)
        );
        
        return ResponseEntity.notFound().build(); // Return empty body for 404s
    }
    
    /**
     * Handles IllegalArgumentException.
     */
    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> handleIllegalArgumentException(
            IllegalArgumentException ex, WebRequest request) {
        log.warn("Invalid argument: {}", ex.getMessage());
        
        Map<String, Object> errorResponse = createErrorResponse(
                HttpStatus.BAD_REQUEST,
                "Invalid request parameter",
                ex.getMessage(),
                request.getDescription(false)
        );
        
        return ResponseEntity.badRequest().body(errorResponse);
    }
    
    /**
     * Handles general exceptions.
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleGeneralException(
            Exception ex, WebRequest request) {
        log.error("Unexpected error: {}", ex.getMessage(), ex);
        
        Map<String, Object> errorResponse = createErrorResponse(
                HttpStatus.INTERNAL_SERVER_ERROR,
                "Internal server error",
                "An unexpected error occurred",
                request.getDescription(false)
        );
        
        return ResponseEntity.internalServerError().body(errorResponse);
    }
    
    /**
     * Creates a standardized error response.
     */
    private Map<String, Object> createErrorResponse(HttpStatus status, String error, String message, String path) {
        Map<String, Object> errorResponse = new HashMap<>();
        errorResponse.put("timestamp", Instant.now());
        errorResponse.put("status", status.value());
        errorResponse.put("error", error);
        errorResponse.put("message", message);
        errorResponse.put("path", path);
        return errorResponse;
    }
}
