package org.hiast.analyticsapi.adapter.in.web;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.method.annotation.MethodArgumentTypeMismatchException;

import jakarta.validation.ConstraintViolation;
import jakarta.validation.ConstraintViolationException;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for the GlobalExceptionHandler class.
 */
class GlobalExceptionHandlerTest {

    private GlobalExceptionHandler exceptionHandler;

    @BeforeEach
    void setUp() {
        exceptionHandler = new GlobalExceptionHandler();
    }

    @Test
    void handleIllegalArgumentException_ShouldReturnBadRequest() {
        // Arrange
        String errorMessage = "Invalid argument provided";
        IllegalArgumentException exception = new IllegalArgumentException(errorMessage);

        // Act
        ResponseEntity<GlobalExceptionHandler.ErrorResponse> response = 
                exceptionHandler.handleIllegalArgumentException(exception);

        // Assert
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals("INVALID_ARGUMENT", response.getBody().getCode());
        assertEquals(errorMessage, response.getBody().getMessage());
        assertNotNull(response.getBody().getTimestamp());
    }

    @Test
    void handleTypeMismatchException_ShouldReturnBadRequest() {
        // Arrange
        MethodArgumentTypeMismatchException exception = mock(MethodArgumentTypeMismatchException.class);
        when(exception.getName()).thenReturn("testParam");
        when(exception.getValue()).thenReturn("invalidValue");

        // Act
        ResponseEntity<GlobalExceptionHandler.ErrorResponse> response = 
                exceptionHandler.handleTypeMismatchException(exception);

        // Assert
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals("INVALID_PARAMETER", response.getBody().getCode());
        assertTrue(response.getBody().getMessage().contains("invalidValue"));
        assertTrue(response.getBody().getMessage().contains("testParam"));
        assertNotNull(response.getBody().getTimestamp());
    }

    @Test
    void handleValidationException_ShouldReturnBadRequest() {
        // Arrange
        MethodArgumentNotValidException exception = mock(MethodArgumentNotValidException.class);

        // Act
        ResponseEntity<GlobalExceptionHandler.ErrorResponse> response = 
                exceptionHandler.handleValidationException(exception);

        // Assert
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals("VALIDATION_ERROR", response.getBody().getCode());
        assertEquals("Request validation failed", response.getBody().getMessage());
        assertNotNull(response.getBody().getTimestamp());
    }

    @Test
    void handleConstraintViolationException_ShouldReturnBadRequest() {
        // Arrange
        Set<ConstraintViolation<?>> violations = new HashSet<>();
        ConstraintViolationException exception = mock(ConstraintViolationException.class);
        when(exception.getMessage()).thenReturn("Constraint violation occurred");

        // Act
        ResponseEntity<GlobalExceptionHandler.ErrorResponse> response = 
                exceptionHandler.handleConstraintViolationException(exception);

        // Assert
        assertEquals(HttpStatus.BAD_REQUEST, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals("CONSTRAINT_VIOLATION", response.getBody().getCode());
        assertEquals("Constraint violation occurred", response.getBody().getMessage());
        assertNotNull(response.getBody().getTimestamp());
    }

    @Test
    void handleGenericException_ShouldReturnInternalServerError() {
        // Arrange
        Exception exception = new RuntimeException("Unexpected error");

        // Act
        ResponseEntity<GlobalExceptionHandler.ErrorResponse> response = 
                exceptionHandler.handleGenericException(exception);

        // Assert
        assertEquals(HttpStatus.INTERNAL_SERVER_ERROR, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals("INTERNAL_ERROR", response.getBody().getCode());
        assertEquals("An unexpected error occurred", response.getBody().getMessage());
        assertNotNull(response.getBody().getTimestamp());
    }

    @Test
    void errorResponse_ShouldHaveCorrectProperties() {
        // Arrange
        String code = "TEST_CODE";
        String message = "Test message";
        String timestamp = "2023-01-01T00:00:00Z";

        // Act
        GlobalExceptionHandler.ErrorResponse errorResponse = 
                new GlobalExceptionHandler.ErrorResponse(code, message, timestamp);

        // Assert
        assertEquals(code, errorResponse.getCode());
        assertEquals(message, errorResponse.getMessage());
        assertEquals(timestamp, errorResponse.getTimestamp());
    }
}