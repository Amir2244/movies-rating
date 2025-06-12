package org.hiast.analyticsapi.adapter.out.persistence;

import org.hiast.analyticsapi.application.port.out.LoadAnalyticsPort;
import org.hiast.analyticsapi.domain.model.AnalyticsQuery;
import org.hiast.model.AnalyticsType;
import org.hiast.model.DataAnalytics;
import org.springframework.data.domain.Page;
import org.springframework.data.domain.PageImpl;
import org.springframework.data.domain.PageRequest;
import org.springframework.data.domain.Pageable;
import org.springframework.data.domain.Sort;
import org.springframework.data.mongodb.core.MongoTemplate;
import org.springframework.data.mongodb.core.aggregation.Aggregation;
import org.springframework.data.mongodb.core.aggregation.AggregationResults;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.stream.Collectors;

/**
 * MongoDB adapter implementing LoadAnalyticsPort.
 * Handles the persistence layer concerns and domain model mapping.
 */
@Component
public class MongoAnalyticsAdapter implements LoadAnalyticsPort {

    private final AnalyticsRepository repository;
    private final MongoTemplate mongoTemplate;
    private final AnalyticsDocumentMapper mapper;

    public MongoAnalyticsAdapter(AnalyticsRepository repository, 
                                MongoTemplate mongoTemplate,
                                AnalyticsDocumentMapper mapper) {
        this.repository = repository;
        this.mongoTemplate = mongoTemplate;
        this.mapper = mapper;
    }

    @Override
    public List<DataAnalytics> loadAnalytics(AnalyticsQuery query) {
        Page<AnalyticsDocument> documentsPage = findDocuments(query);
        return documentsPage.getContent().stream()
                .map(mapper::toDomain)
                .collect(Collectors.toList());
    }

    @Override
    public long countAnalytics(AnalyticsQuery query) {
        return countDocuments(query);
    }

    @Override
    public Optional<DataAnalytics> loadAnalyticsById(String analyticsId) {
        return repository.findByAnalyticsId(analyticsId)
                .map(mapper::toDomain);
    }

    @Override
    public Map<AnalyticsType, Long> loadAnalyticsCountsByType() {
        // Use aggregation to group by type and count
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.group("type").count().as("count"),
                Aggregation.project("count").and("_id").as("type")
        );

        AggregationResults<TypeCountResult> results = mongoTemplate.aggregate(
                aggregation, "analytics", TypeCountResult.class);

        Map<AnalyticsType, Long> countsByType = new HashMap<>();
        for (TypeCountResult result : results.getMappedResults()) {
            try {
                AnalyticsType type = AnalyticsType.valueOf(result.getType());
                countsByType.put(type, result.getCount());
            } catch (IllegalArgumentException e) {
                // Skip unknown types
            }
        }
        return countsByType;
    }

    @Override
    public Map<String, Long> loadAnalyticsCountsByCategory() {
        Map<AnalyticsType, Long> countsByType = loadAnalyticsCountsByType();
        return countsByType.entrySet().stream()
                .collect(Collectors.groupingBy(
                        entry -> entry.getKey().getCategory(),
                        Collectors.summingLong(Map.Entry::getValue)
                ));
    }

    @Override
    public Optional<String> getLatestAnalyticsTimestamp() {
        return repository.findTopByOrderByGeneratedAtDesc()
                .map(doc -> doc.getGeneratedAtAsInstant().toString());
    }

    @Override
    public Optional<String> getOldestAnalyticsTimestamp() {
        return repository.findTopByOrderByGeneratedAtAsc()
                .map(doc -> doc.getGeneratedAtAsInstant().toString());
    }

    @Override
    public long getTotalAnalyticsCount() {
        return repository.count();
    }

    @Override
    public long getUniqueAnalyticsTypesCount() {
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.group("type"),
                Aggregation.count().as("uniqueTypes")
        );

        AggregationResults<CountResult> results = mongoTemplate.aggregate(
                aggregation, "analytics", CountResult.class);

        return results.getMappedResults().isEmpty() ? 0 : 
               results.getMappedResults().get(0).getUniqueTypes();
    }

    @Override
    public boolean existsByType(AnalyticsType type) {
        return repository.existsByType(type.name());
    }

    @Override
    public List<DataAnalytics> loadLatestAnalyticsByType() {
        // Use aggregation to get the latest analytics for each type
        Aggregation aggregation = Aggregation.newAggregation(
                Aggregation.sort(Sort.by(Sort.Direction.DESC, "generatedAt")),
                Aggregation.group("type")
                        .first("analyticsId").as("analyticsId")
                        .first("generatedAt").as("generatedAt")
                        .first("type").as("type")
                        .first("metrics").as("metrics")
                        .first("description").as("description")
        );

        AggregationResults<AnalyticsDocument> results = mongoTemplate.aggregate(
                aggregation, "analytics", AnalyticsDocument.class);

        return results.getMappedResults().stream()
                .map(mapper::toDomain)
                .collect(Collectors.toList());
    }

    private Page<AnalyticsDocument> findDocuments(AnalyticsQuery query) {
        Pageable pageable = createPageable(query);
        
        if (query.getAnalyticsId().isPresent()) {
            // Single analytics by ID
            return repository.findByAnalyticsId(query.getAnalyticsId().get())
                    .map(doc -> (Page<AnalyticsDocument>) new PageImpl<>(Collections.singletonList(doc), pageable, 1))
                    .orElse(Page.empty(pageable));
        }

        if (query.hasTypeFilters() && query.hasDateRange()) {
            // Type and date filters
            List<String> typeNames = query.getTypes().get().stream()
                    .map(Enum::name)
                    .collect(Collectors.toList());
            return repository.findByTypeInAndGeneratedAtBetween(
                    typeNames,
                    Date.from(query.getFromDate().orElse(new Date(0).toInstant())),
                    Date.from(query.getToDate().orElse(new Date().toInstant())),
                    pageable
            );
        }

        if (query.hasTypeFilters()) {
            // Type filters only
            List<String> typeNames = query.getTypes().get().stream()
                    .map(Enum::name)
                    .collect(Collectors.toList());
            return repository.findByTypeIn(typeNames, pageable);
        }

        if (query.hasDateRange()) {
            // Date range filters only
            return repository.findByGeneratedAtBetween(
                    Date.from(query.getFromDate().orElse(new Date(0).toInstant())),
                    Date.from(query.getToDate().orElse(new Date().toInstant())),
                    pageable
            );
        }

        if (query.getDescriptionKeyword().isPresent()) {
            // Description keyword search
            return repository.findByDescriptionContainingIgnoreCase(
                    query.getDescriptionKeyword().get(), pageable);
        }

        // No filters - return all
        return repository.findAll(pageable);
    }

    private long countDocuments(AnalyticsQuery query) {
        if (query.getAnalyticsId().isPresent()) {
            return repository.findByAnalyticsId(query.getAnalyticsId().get()).isPresent() ? 1 : 0;
        }

        if (query.hasTypeFilters() && query.hasDateRange()) {
            List<String> typeNames = query.getTypes().get().stream()
                    .map(Enum::name)
                    .collect(Collectors.toList());
            return repository.countByTypeInAndGeneratedAtBetween(
                    typeNames,
                    Date.from(query.getFromDate().orElse(new Date(0).toInstant())),
                    Date.from(query.getToDate().orElse(new Date().toInstant()))
            );
        }

        if (query.hasTypeFilters()) {
            return query.getTypes().get().stream()
                    .mapToLong(type -> repository.countByType(type.name()))
                    .sum();
        }

        if (query.hasDateRange()) {
            return repository.countByGeneratedAtBetween(
                    Date.from(query.getFromDate().orElse(new Date(0).toInstant())),
                    Date.from(query.getToDate().orElse(new Date().toInstant()))
            );
        }

        return repository.count();
    }

    private Pageable createPageable(AnalyticsQuery query) {
        Sort sort = Sort.by(
                query.getSortDirection() == AnalyticsQuery.SortDirection.ASC ?
                        Sort.Direction.ASC : Sort.Direction.DESC,
                query.getSortBy()
        );
        return PageRequest.of(query.getPage(), query.getSize(), sort);
    }

    // Helper classes for aggregation results
    public static class TypeCountResult {
        private String type;
        private Long count;

        public String getType() {
            return type;
        }

        public void setType(String type) {
            this.type = type;
        }

        public Long getCount() {
            return count;
        }

        public void setCount(Long count) {
            this.count = count;
        }
    }

    public static class CountResult {
        private Long uniqueTypes;

        public Long getUniqueTypes() {
            return uniqueTypes;
        }

        public void setUniqueTypes(Long uniqueTypes) {
            this.uniqueTypes = uniqueTypes;
        }
    }
} 