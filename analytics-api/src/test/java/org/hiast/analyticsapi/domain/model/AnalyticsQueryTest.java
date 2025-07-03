package org.hiast.analyticsapi.domain.model;

import org.hiast.model.AnalyticsType;
import org.junit.jupiter.api.Test;

import java.time.Instant;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Unit tests for the AnalyticsQuery domain model.
 */
class AnalyticsQueryTest {

    @Test
    void testBuilderCreatesQueryWithCorrectValues() {
        // Arrange
        List<AnalyticsType> types = Arrays.asList(AnalyticsType.USER_ACTIVITY, AnalyticsType.CONTENT_PERFORMANCE);
        Instant fromDate = Instant.parse("2023-01-01T00:00:00Z");
        Instant toDate = Instant.parse("2023-12-31T23:59:59Z");
        String analyticsId = "test-id";
        String descriptionKeyword = "test-keyword";
        int page = 2;
        int size = 50;
        String sortBy = "type";
        AnalyticsQuery.SortDirection sortDirection = AnalyticsQuery.SortDirection.ASC;

        // Act
        AnalyticsQuery query = AnalyticsQuery.builder()
                .withTypes(types)
                .withFromDate(fromDate)
                .withToDate(toDate)
                .withAnalyticsId(analyticsId)
                .withDescriptionKeyword(descriptionKeyword)
                .withPagination(page, size)
                .withSorting(sortBy, sortDirection)
                .build();

        // Assert
        assertEquals(types, query.getTypes().orElse(null));
        assertEquals(fromDate, query.getFromDate().orElse(null));
        assertEquals(toDate, query.getToDate().orElse(null));
        assertEquals(analyticsId, query.getAnalyticsId().orElse(null));
        assertEquals(descriptionKeyword, query.getDescriptionKeyword().orElse(null));
        assertEquals(page, query.getPage());
        assertEquals(size, query.getSize());
        assertEquals(sortBy, query.getSortBy());
        assertEquals(sortDirection, query.getSortDirection());
    }

    @Test
    void testDefaultValuesWhenNotSpecified() {
        // Act
        AnalyticsQuery query = AnalyticsQuery.builder().build();

        // Assert
        assertFalse(query.getTypes().isPresent());
        assertFalse(query.getFromDate().isPresent());
        assertFalse(query.getToDate().isPresent());
        assertFalse(query.getAnalyticsId().isPresent());
        assertFalse(query.getDescriptionKeyword().isPresent());
        assertEquals(0, query.getPage());
        assertEquals(20, query.getSize());
        assertEquals("generatedAt", query.getSortBy());
        assertEquals(AnalyticsQuery.SortDirection.DESC, query.getSortDirection());
    }

    @Test
    void testHasDateRangeReturnsTrueWhenDatesAreSet() {
        // Arrange
        Instant fromDate = Instant.parse("2023-01-01T00:00:00Z");
        Instant toDate = Instant.parse("2023-12-31T23:59:59Z");

        // Act & Assert
        assertTrue(AnalyticsQuery.builder().withFromDate(fromDate).build().hasDateRange());
        assertTrue(AnalyticsQuery.builder().withToDate(toDate).build().hasDateRange());
        assertTrue(AnalyticsQuery.builder().withDateRange(fromDate, toDate).build().hasDateRange());
        assertFalse(AnalyticsQuery.builder().build().hasDateRange());
    }

    @Test
    void testHasTypeFiltersReturnsTrueWhenTypesAreSet() {
        // Arrange
        List<AnalyticsType> types = Arrays.asList(AnalyticsType.USER_ACTIVITY);

        // Act & Assert
        assertTrue(AnalyticsQuery.builder().withTypes(types).build().hasTypeFilters());
        assertFalse(AnalyticsQuery.builder().build().hasTypeFilters());
        assertFalse(AnalyticsQuery.builder().withTypes(Arrays.asList()).build().hasTypeFilters());
    }

    @Test
    void testEqualsAndHashCode() {
        // Arrange
        List<AnalyticsType> types = Arrays.asList(AnalyticsType.USER_ACTIVITY);
        Instant fromDate = Instant.parse("2023-01-01T00:00:00Z");

        AnalyticsQuery query1 = AnalyticsQuery.builder()
                .withTypes(types)
                .withFromDate(fromDate)
                .build();

        AnalyticsQuery query2 = AnalyticsQuery.builder()
                .withTypes(types)
                .withFromDate(fromDate)
                .build();

        AnalyticsQuery query3 = AnalyticsQuery.builder()
                .withTypes(Arrays.asList(AnalyticsType.DATA_QUALITY))
                .withFromDate(fromDate)
                .build();

        // Act & Assert
        assertEquals(query1, query2);
        assertEquals(query1.hashCode(), query2.hashCode());
        assertNotEquals(query1, query3);
        assertNotEquals(query1.hashCode(), query3.hashCode());
    }
}
