package org.hiast.analyticsapi.adapter.in.web;

import org.hiast.analyticsapi.application.port.in.GetAnalyticsUseCase;
import org.hiast.analyticsapi.domain.model.AnalyticsQuery;
import org.hiast.model.AnalyticsType;
import org.hiast.model.DataAnalytics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * Unit tests for the AnalyticsController.
 */
@ExtendWith(MockitoExtension.class)
class AnalyticsControllerTest {

    @Mock
    private GetAnalyticsUseCase getAnalyticsUseCase;

    private AnalyticsController analyticsController;

    @BeforeEach
    void setUp() {
        analyticsController = new AnalyticsController(getAnalyticsUseCase);
    }

    @Test
    void getAnalytics_ShouldReturnPaginatedResult() {
        // Arrange
        List<String> types = Arrays.asList("USER_ACTIVITY", "DATA_QUALITY");
        LocalDateTime fromDate = LocalDateTime.of(2023, 1, 1, 0, 0);
        LocalDateTime toDate = LocalDateTime.of(2023, 12, 31, 23, 59);
        String keyword = "test";
        int page = 1;
        int size = 10;
        String sortBy = "type";
        String sortDirection = "ASC";

        List<DataAnalytics> mockAnalytics = Arrays.asList(
                createMockAnalytics("1", AnalyticsType.USER_ACTIVITY),
                createMockAnalytics("2", AnalyticsType.DATA_QUALITY)
        );

        GetAnalyticsUseCase.AnalyticsResult mockResult = new GetAnalyticsUseCase.AnalyticsResult(
                mockAnalytics, 25L, 3, page, size
        );

        when(getAnalyticsUseCase.getAnalytics(any(AnalyticsQuery.class))).thenReturn(mockResult);

        // Act
        ResponseEntity<GetAnalyticsUseCase.AnalyticsResult> response = analyticsController.getAnalytics(
                types, fromDate, toDate, keyword, page, size, sortBy, sortDirection
        );

        // Assert
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(mockResult, response.getBody());

        // Verify query construction
        ArgumentCaptor<AnalyticsQuery> queryCaptor = ArgumentCaptor.forClass(AnalyticsQuery.class);
        verify(getAnalyticsUseCase).getAnalytics(queryCaptor.capture());
        
        AnalyticsQuery capturedQuery = queryCaptor.getValue();
        assertEquals(page, capturedQuery.getPage());
        assertEquals(size, capturedQuery.getSize());
        assertEquals(sortBy, capturedQuery.getSortBy());
        assertEquals(AnalyticsQuery.SortDirection.ASC, capturedQuery.getSortDirection());
        
        // Verify type filters
        assertTrue(capturedQuery.hasTypeFilters());
        List<AnalyticsType> capturedTypes = capturedQuery.getTypes().orElse(Collections.emptyList());
        assertEquals(2, capturedTypes.size());
        assertTrue(capturedTypes.contains(AnalyticsType.USER_ACTIVITY));
        assertTrue(capturedTypes.contains(AnalyticsType.DATA_QUALITY));
        
        // Verify date range
        assertTrue(capturedQuery.hasDateRange());
        assertEquals(fromDate.toInstant(ZoneOffset.UTC), capturedQuery.getFromDate().orElse(null));
        assertEquals(toDate.toInstant(ZoneOffset.UTC), capturedQuery.getToDate().orElse(null));
        
        // Verify keyword
        assertEquals(keyword, capturedQuery.getDescriptionKeyword().orElse(null));
    }

    @Test
    void getAnalyticsById_ShouldReturnAnalyticsWhenFound() {
        // Arrange
        String analyticsId = "test-id";
        DataAnalytics mockAnalytics = createMockAnalytics(analyticsId, AnalyticsType.USER_ACTIVITY);
        
        when(getAnalyticsUseCase.getAnalyticsById(analyticsId)).thenReturn(Optional.of(mockAnalytics));

        // Act
        ResponseEntity<DataAnalytics> response = analyticsController.getAnalyticsById(analyticsId);

        // Assert
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(mockAnalytics, response.getBody());
        
        verify(getAnalyticsUseCase).getAnalyticsById(analyticsId);
    }

    @Test
    void getAnalyticsById_ShouldReturnNotFoundWhenNotFound() {
        // Arrange
        String analyticsId = "non-existent-id";
        
        when(getAnalyticsUseCase.getAnalyticsById(analyticsId)).thenReturn(Optional.empty());

        // Act
        ResponseEntity<DataAnalytics> response = analyticsController.getAnalyticsById(analyticsId);

        // Assert
        assertEquals(HttpStatus.NOT_FOUND, response.getStatusCode());
        assertNull(response.getBody());
        
        verify(getAnalyticsUseCase).getAnalyticsById(analyticsId);
    }

    @Test
    void getAnalyticsTypes_ShouldReturnTypesList() {
        // Arrange
        List<GetAnalyticsUseCase.AnalyticsTypeInfo> mockTypes = Arrays.asList(
                new GetAnalyticsUseCase.AnalyticsTypeInfo("USER_ACTIVITY", "User Behavior", "Description", 10L),
                new GetAnalyticsUseCase.AnalyticsTypeInfo("DATA_QUALITY", "Data Quality", "Description", 5L)
        );
        
        when(getAnalyticsUseCase.getAvailableAnalyticsTypes()).thenReturn(mockTypes);

        // Act
        ResponseEntity<List<GetAnalyticsUseCase.AnalyticsTypeInfo>> response = analyticsController.getAnalyticsTypes();

        // Assert
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(mockTypes, response.getBody());
        
        verify(getAnalyticsUseCase).getAvailableAnalyticsTypes();
    }

    @Test
    void getAnalyticsSummary_ShouldReturnSummary() {
        // Arrange
        List<GetAnalyticsUseCase.AnalyticsSummary.CategorySummary> categorySummaries = Arrays.asList(
                new GetAnalyticsUseCase.AnalyticsSummary.CategorySummary("User Behavior", 60L, 60.0),
                new GetAnalyticsUseCase.AnalyticsSummary.CategorySummary("Data Quality", 40L, 40.0)
        );
        
        GetAnalyticsUseCase.AnalyticsSummary mockSummary = new GetAnalyticsUseCase.AnalyticsSummary(
                100L, 5L, "2023-12-31T23:59:59Z", "2023-01-01T00:00:00Z", categorySummaries
        );
        
        when(getAnalyticsUseCase.getAnalyticsSummary()).thenReturn(mockSummary);

        // Act
        ResponseEntity<GetAnalyticsUseCase.AnalyticsSummary> response = analyticsController.getAnalyticsSummary();

        // Assert
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertEquals(mockSummary, response.getBody());
        
        verify(getAnalyticsUseCase).getAnalyticsSummary();
    }

    @Test
    void getAllAnalyticsTypeEnums_ShouldReturnAllTypes() {
        // Act
        ResponseEntity<List<AnalyticsController.AnalyticsTypeResponse>> response = analyticsController.getAllAnalyticsTypeEnums();

        // Assert
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        
        // Verify all enum values are included
        List<AnalyticsController.AnalyticsTypeResponse> types = response.getBody();
        assertEquals(AnalyticsType.values().length, types.size());
        
        // Verify a few specific types
        assertTrue(types.stream().anyMatch(t -> "USER_ACTIVITY".equals(t.getName())));
        assertTrue(types.stream().anyMatch(t -> "DATA_QUALITY".equals(t.getName())));
    }

    @Test
    void health_ShouldReturnOkStatus() {
        // Act
        ResponseEntity<AnalyticsController.HealthResponse> response = analyticsController.health();

        // Assert
        assertEquals(HttpStatus.OK, response.getStatusCode());
        assertNotNull(response.getBody());
        assertEquals("OK", response.getBody().getStatus());
        assertEquals("Analytics API is running", response.getBody().getMessage());
    }

    /**
     * Helper method to create a mock DataAnalytics object.
     */
    private DataAnalytics createMockAnalytics(String id, AnalyticsType type) {
        return new DataAnalytics(
            id,
            Instant.now(),
            type,
            Collections.singletonMap("testMetric", 42.0),
            "Test analytics " + id
        );
    }
}