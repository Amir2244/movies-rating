package org.hiast.analyticsapi.application.service;

import org.hiast.analyticsapi.application.port.in.GetAnalyticsUseCase;
import org.hiast.analyticsapi.application.port.out.LoadAnalyticsPort;
import org.hiast.analyticsapi.domain.model.AnalyticsQuery;
import org.hiast.model.AnalyticsType;
import org.hiast.model.DataAnalytics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for the AnalyticsService application service.
 */
@ExtendWith(MockitoExtension.class)
class AnalyticsServiceTest {

    @Mock
    private LoadAnalyticsPort loadAnalyticsPort;

    private AnalyticsService analyticsService;

    @BeforeEach
    void setUp() {
        analyticsService = new AnalyticsService(loadAnalyticsPort);
    }

    @Test
    void getAnalytics_ShouldReturnPaginatedResult() {
        // Arrange
        AnalyticsQuery query = AnalyticsQuery.builder()
                .withPagination(1, 10)
                .build();

        List<DataAnalytics> mockAnalytics = Arrays.asList(
                createMockAnalytics("1", AnalyticsType.USER_ACTIVITY),
                createMockAnalytics("2", AnalyticsType.DATA_QUALITY)
        );

        when(loadAnalyticsPort.loadAnalytics(query)).thenReturn(mockAnalytics);
        when(loadAnalyticsPort.countAnalytics(query)).thenReturn(25L); // Total of 25 records

        // Act
        GetAnalyticsUseCase.AnalyticsResult result = analyticsService.getAnalytics(query);

        // Assert
        assertEquals(mockAnalytics, result.getAnalytics());
        assertEquals(25L, result.getTotalElements());
        assertEquals(3, result.getTotalPages()); // 25 records with page size 10 = 3 pages
        assertEquals(1, result.getCurrentPage());
        assertEquals(10, result.getPageSize());
        assertTrue(result.hasNext());
        assertTrue(result.hasPrevious());

        verify(loadAnalyticsPort).loadAnalytics(query);
        verify(loadAnalyticsPort).countAnalytics(query);
    }

    @Test
    void getAnalyticsById_ShouldReturnAnalytics() {
        // Arrange
        String analyticsId = "test-id";
        DataAnalytics mockAnalytics = createMockAnalytics(analyticsId, AnalyticsType.USER_ACTIVITY);

        when(loadAnalyticsPort.loadAnalyticsById(analyticsId)).thenReturn(Optional.of(mockAnalytics));

        // Act
        Optional<DataAnalytics> result = analyticsService.getAnalyticsById(analyticsId);

        // Assert
        assertTrue(result.isPresent());
        assertEquals(mockAnalytics, result.get());

        verify(loadAnalyticsPort).loadAnalyticsById(analyticsId);
    }

    @Test
    void getAvailableAnalyticsTypes_ShouldReturnTypesWithCounts() {
        // Arrange
        Map<AnalyticsType, Long> mockCountsByType = new HashMap<>();
        mockCountsByType.put(AnalyticsType.USER_ACTIVITY, 10L);
        mockCountsByType.put(AnalyticsType.DATA_QUALITY, 5L);

        when(loadAnalyticsPort.loadAnalyticsCountsByType()).thenReturn(mockCountsByType);

        // Act
        List<GetAnalyticsUseCase.AnalyticsTypeInfo> result = analyticsService.getAvailableAnalyticsTypes();

        // Assert
        assertEquals(2, result.size());

        // Find and verify USER_ACTIVITY type info
        GetAnalyticsUseCase.AnalyticsTypeInfo userActivityInfo = result.stream()
                .filter(info -> "USER_ACTIVITY".equals(info.getType()))
                .findFirst()
                .orElse(null);
        assertNotNull(userActivityInfo);
        assertEquals("User Behavior", userActivityInfo.getCategory());
        assertEquals(10L, userActivityInfo.getCount());

        // Find and verify DATA_QUALITY type info
        GetAnalyticsUseCase.AnalyticsTypeInfo dataQualityInfo = result.stream()
                .filter(info -> "DATA_QUALITY".equals(info.getType()))
                .findFirst()
                .orElse(null);
        assertNotNull(dataQualityInfo);
        assertEquals("Data Quality", dataQualityInfo.getCategory());
        assertEquals(5L, dataQualityInfo.getCount());

        verify(loadAnalyticsPort).loadAnalyticsCountsByType();
    }

    @Test
    void getAnalyticsSummary_ShouldReturnSummary() {
        // Arrange
        when(loadAnalyticsPort.getTotalAnalyticsCount()).thenReturn(100L);
        when(loadAnalyticsPort.getUniqueAnalyticsTypesCount()).thenReturn(5L);
        when(loadAnalyticsPort.getLatestAnalyticsTimestamp()).thenReturn(Optional.of("2023-12-31T23:59:59Z"));
        when(loadAnalyticsPort.getOldestAnalyticsTimestamp()).thenReturn(Optional.of("2023-01-01T00:00:00Z"));

        Map<String, Long> mockCountsByCategory = new HashMap<>();
        mockCountsByCategory.put("User Behavior", 60L);
        mockCountsByCategory.put("Data Quality", 40L);
        when(loadAnalyticsPort.loadAnalyticsCountsByCategory()).thenReturn(mockCountsByCategory);

        // Act
        GetAnalyticsUseCase.AnalyticsSummary result = analyticsService.getAnalyticsSummary();

        // Assert
        assertEquals(100L, result.getTotalAnalytics());
        assertEquals(5L, result.getUniqueTypes());
        assertEquals("2023-12-31T23:59:59Z", result.getLatestAnalyticsDate());
        assertEquals("2023-01-01T00:00:00Z", result.getOldestAnalyticsDate());

        assertEquals(2, result.getCategorySummaries().size());

        // Find and verify User Behavior category summary
        GetAnalyticsUseCase.AnalyticsSummary.CategorySummary userBehaviorSummary = result.getCategorySummaries().stream()
                .filter(summary -> "User Behavior".equals(summary.getCategory()))
                .findFirst()
                .orElse(null);
        assertNotNull(userBehaviorSummary);
        assertEquals(60L, userBehaviorSummary.getCount());
        assertEquals(60.0, userBehaviorSummary.getPercentage());

        // Find and verify Data Quality category summary
        GetAnalyticsUseCase.AnalyticsSummary.CategorySummary dataQualitySummary = result.getCategorySummaries().stream()
                .filter(summary -> "Data Quality".equals(summary.getCategory()))
                .findFirst()
                .orElse(null);
        assertNotNull(dataQualitySummary);
        assertEquals(40L, dataQualitySummary.getCount());
        assertEquals(40.0, dataQualitySummary.getPercentage());

        verify(loadAnalyticsPort).getTotalAnalyticsCount();
        verify(loadAnalyticsPort).getUniqueAnalyticsTypesCount();
        verify(loadAnalyticsPort).getLatestAnalyticsTimestamp();
        verify(loadAnalyticsPort).getOldestAnalyticsTimestamp();
        verify(loadAnalyticsPort).loadAnalyticsCountsByCategory();
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
