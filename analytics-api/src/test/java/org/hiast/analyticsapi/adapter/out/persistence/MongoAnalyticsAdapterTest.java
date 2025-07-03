package org.hiast.analyticsapi.adapter.out.persistence;

import org.hiast.analyticsapi.domain.model.AnalyticsQuery;
import org.hiast.model.AnalyticsType;
import org.hiast.model.DataAnalytics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;

import java.time.Instant;
import java.util.*;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

/**
 * Unit tests for the MongoAnalyticsAdapter.
 */
@ExtendWith(MockitoExtension.class)
class MongoAnalyticsAdapterTest {

    @Mock
    private AnalyticsRepository repository;

    @Mock
    private MongoTemplate mongoTemplate;

    @Mock
    private AnalyticsDocumentMapper mapper;

    private MongoAnalyticsAdapter adapter;

    @BeforeEach
    void setUp() {
        adapter = new MongoAnalyticsAdapter(repository, mongoTemplate, mapper);
    }

    @Test
    void loadAnalytics_ShouldReturnMappedDomainObjects() {
        // Arrange
        AnalyticsQuery query = AnalyticsQuery.builder()
                .withPagination(0, 10)
                .build();

        AnalyticsDocument document1 = mock(AnalyticsDocument.class);
        AnalyticsDocument document2 = mock(AnalyticsDocument.class);
        List<AnalyticsDocument> documents = Arrays.asList(document1, document2);

        Page<AnalyticsDocument> page = new PageImpl<>(documents);
        when(repository.findAll(any(Pageable.class))).thenReturn(page);

        DataAnalytics analytics1 = createMockAnalytics("1", AnalyticsType.USER_ACTIVITY);
        DataAnalytics analytics2 = createMockAnalytics("2", AnalyticsType.DATA_QUALITY);
        when(mapper.toDomain(document1)).thenReturn(analytics1);
        when(mapper.toDomain(document2)).thenReturn(analytics2);

        // Act
        List<DataAnalytics> result = adapter.loadAnalytics(query);

        // Assert
        assertEquals(2, result.size());
        assertEquals(analytics1, result.get(0));
        assertEquals(analytics2, result.get(1));

        verify(repository).findAll(any(Pageable.class));
        verify(mapper).toDomain(document1);
        verify(mapper).toDomain(document2);
    }

    @Test
    void loadAnalyticsById_ShouldReturnMappedDomainObject() {
        // Arrange
        String analyticsId = "test-id";
        AnalyticsDocument document = mock(AnalyticsDocument.class);
        DataAnalytics analytics = createMockAnalytics(analyticsId, AnalyticsType.USER_ACTIVITY);

        when(repository.findByAnalyticsId(analyticsId)).thenReturn(Optional.of(document));
        when(mapper.toDomain(document)).thenReturn(analytics);

        // Act
        Optional<DataAnalytics> result = adapter.loadAnalyticsById(analyticsId);

        // Assert
        assertTrue(result.isPresent());
        assertEquals(analytics, result.get());

        verify(repository).findByAnalyticsId(analyticsId);
        verify(mapper).toDomain(document);
    }

    @Test
    void loadAnalyticsCountsByType_ShouldReturnCountsByType() {
        // Arrange
        List<MongoAnalyticsAdapter.TypeCountResult> aggregationResults = Arrays.asList(
                createTypeCountResult(AnalyticsType.USER_ACTIVITY.name(), 10L),
                createTypeCountResult(AnalyticsType.DATA_QUALITY.name(), 5L)
        );

        AggregationResults<MongoAnalyticsAdapter.TypeCountResult> results = mock(AggregationResults.class);
        when(results.getMappedResults()).thenReturn(aggregationResults);
        when(mongoTemplate.aggregate(any(Aggregation.class), eq("analytics"), eq(MongoAnalyticsAdapter.TypeCountResult.class)))
                .thenReturn(results);

        // Act
        Map<AnalyticsType, Long> countsByType = adapter.loadAnalyticsCountsByType();

        // Assert
        assertEquals(2, countsByType.size());
        assertEquals(10L, countsByType.get(AnalyticsType.USER_ACTIVITY));
        assertEquals(5L, countsByType.get(AnalyticsType.DATA_QUALITY));

        verify(mongoTemplate).aggregate(any(Aggregation.class), eq("analytics"), eq(MongoAnalyticsAdapter.TypeCountResult.class));
    }

    @Test
    void loadAnalyticsCountsByCategory_ShouldReturnCountsByCategory() {
        // Arrange
        Map<AnalyticsType, Long> countsByType = new HashMap<>();
        countsByType.put(AnalyticsType.USER_ACTIVITY, 10L);
        countsByType.put(AnalyticsType.USER_ENGAGEMENT, 5L);
        countsByType.put(AnalyticsType.DATA_QUALITY, 7L);

        // Mock the loadAnalyticsCountsByType method
        MongoAnalyticsAdapter spyAdapter = spy(adapter);
        doReturn(countsByType).when(spyAdapter).loadAnalyticsCountsByType();

        // Act
        Map<String, Long> countsByCategory = spyAdapter.loadAnalyticsCountsByCategory();

        // Assert
        assertEquals(2, countsByCategory.size());
        assertEquals(15L, countsByCategory.get("User Behavior")); // USER_ACTIVITY + USER_ENGAGEMENT
        assertEquals(7L, countsByCategory.get("Data Quality")); // DATA_QUALITY

        verify(spyAdapter).loadAnalyticsCountsByType();
    }

    @Test
    void getLatestAnalyticsTimestamp_ShouldReturnFormattedTimestamp() {
        // Arrange
        Instant now = Instant.now();
        AnalyticsDocument document = mock(AnalyticsDocument.class);
        when(document.getGeneratedAtAsInstant()).thenReturn(now);
        when(repository.findTopByOrderByGeneratedAtDesc()).thenReturn(Optional.of(document));

        // Act
        Optional<String> result = adapter.getLatestAnalyticsTimestamp();

        // Assert
        assertTrue(result.isPresent());
        assertEquals(now.toString(), result.get());

        verify(repository).findTopByOrderByGeneratedAtDesc();
    }

    @Test
    void getOldestAnalyticsTimestamp_ShouldReturnFormattedTimestamp() {
        // Arrange
        Instant now = Instant.now();
        AnalyticsDocument document = mock(AnalyticsDocument.class);
        when(document.getGeneratedAtAsInstant()).thenReturn(now);
        when(repository.findTopByOrderByGeneratedAtAsc()).thenReturn(Optional.of(document));

        // Act
        Optional<String> result = adapter.getOldestAnalyticsTimestamp();

        // Assert
        assertTrue(result.isPresent());
        assertEquals(now.toString(), result.get());

        verify(repository).findTopByOrderByGeneratedAtAsc();
    }

    @Test
    void getTotalAnalyticsCount_ShouldReturnRepositoryCount() {
        // Arrange
        when(repository.count()).thenReturn(100L);

        // Act
        long result = adapter.getTotalAnalyticsCount();

        // Assert
        assertEquals(100L, result);
        verify(repository).count();
    }

    @Test
    void getUniqueAnalyticsTypesCount_ShouldReturnAggregationResult() {
        // Arrange
        MongoAnalyticsAdapter.CountResult countResult = new MongoAnalyticsAdapter.CountResult();
        countResult.setUniqueTypes(5L);

        List<MongoAnalyticsAdapter.CountResult> aggregationResults = Collections.singletonList(countResult);
        AggregationResults<MongoAnalyticsAdapter.CountResult> results = mock(AggregationResults.class);
        when(results.getMappedResults()).thenReturn(aggregationResults);
        when(mongoTemplate.aggregate(any(Aggregation.class), eq("analytics"), eq(MongoAnalyticsAdapter.CountResult.class)))
                .thenReturn(results);

        // Act
        long result = adapter.getUniqueAnalyticsTypesCount();

        // Assert
        assertEquals(5L, result);
        verify(mongoTemplate).aggregate(any(Aggregation.class), eq("analytics"), eq(MongoAnalyticsAdapter.CountResult.class));
    }

    @Test
    void existsByType_ShouldReturnRepositoryResult() {
        // Arrange
        when(repository.existsByType(AnalyticsType.USER_ACTIVITY.name())).thenReturn(true);
        when(repository.existsByType(AnalyticsType.DATA_QUALITY.name())).thenReturn(false);

        // Act & Assert
        assertTrue(adapter.existsByType(AnalyticsType.USER_ACTIVITY));
        assertFalse(adapter.existsByType(AnalyticsType.DATA_QUALITY));

        verify(repository).existsByType(AnalyticsType.USER_ACTIVITY.name());
        verify(repository).existsByType(AnalyticsType.DATA_QUALITY.name());
    }

    // Helper methods
    private AnalyticsDocument createMockDocument(String id, String type) {
        AnalyticsDocument document = mock(AnalyticsDocument.class);
        // Don't set up unnecessary stubs
        return document;
    }

    private DataAnalytics createMockAnalytics(String id, AnalyticsType type) {
        return new DataAnalytics(
            id,
            Instant.now(),
            type,
            Collections.singletonMap("testMetric", 42.0),
            "Test analytics " + id
        );
    }

    private MongoAnalyticsAdapter.TypeCountResult createTypeCountResult(String type, Long count) {
        MongoAnalyticsAdapter.TypeCountResult result = new MongoAnalyticsAdapter.TypeCountResult();
        result.setType(type);
        result.setCount(count);
        return result;
    }
}
