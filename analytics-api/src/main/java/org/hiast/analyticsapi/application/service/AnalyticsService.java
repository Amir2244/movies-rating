package org.hiast.analyticsapi.application.service;

import org.hiast.analyticsapi.application.port.in.GetAnalyticsUseCase;
import org.hiast.analyticsapi.application.port.out.LoadAnalyticsPort;
import org.hiast.analyticsapi.domain.model.AnalyticsQuery;
import org.hiast.model.AnalyticsType;
import org.hiast.model.DataAnalytics;
import org.springframework.cache.annotation.Cacheable;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * Application service implementing analytics use cases.
 * This service orchestrates business logic and coordinates with output ports.
 */
@Service
public class AnalyticsService implements GetAnalyticsUseCase {

    private final LoadAnalyticsPort loadAnalyticsPort;

    public AnalyticsService(LoadAnalyticsPort loadAnalyticsPort) {
        this.loadAnalyticsPort = loadAnalyticsPort;
    }

    @Override
    public AnalyticsResult getAnalytics(AnalyticsQuery query) {
        // Load analytics data with pagination
        List<DataAnalytics> analytics = loadAnalyticsPort.loadAnalytics(query);
        
        // Get total count for pagination calculation
        long totalElements = loadAnalyticsPort.countAnalytics(query);
        
        // Calculate pagination info
        int pageSize = query.getSize();
        int currentPage = query.getPage();
        int totalPages = (int) Math.ceil((double) totalElements / pageSize);

        return new AnalyticsResult(analytics, totalElements, totalPages, currentPage, pageSize);
    }

    @Override
    public Optional<DataAnalytics> getAnalyticsById(String analyticsId) {
        return loadAnalyticsPort.loadAnalyticsById(analyticsId);
    }

    @Override
    @Cacheable(value = "analytics", key = "'analytics-types'")
    public List<AnalyticsTypeInfo> getAvailableAnalyticsTypes() {
        Map<AnalyticsType, Long> countsByType = loadAnalyticsPort.loadAnalyticsCountsByType();
        
        return countsByType.entrySet().stream()
                .map(entry -> new AnalyticsTypeInfo(
                        entry.getKey().name(),
                        entry.getKey().getCategory(),
                        entry.getKey().getDescription(),
                        entry.getValue()
                ))
                .collect(Collectors.toList());
    }

    @Override
    public AnalyticsSummary getAnalyticsSummary() {
        // Get basic statistics
        long totalAnalytics = loadAnalyticsPort.getTotalAnalyticsCount();
        long uniqueTypes = loadAnalyticsPort.getUniqueAnalyticsTypesCount();
        
        // Get timestamps
        String latestDate = loadAnalyticsPort.getLatestAnalyticsTimestamp().orElse("N/A");
        String oldestDate = loadAnalyticsPort.getOldestAnalyticsTimestamp().orElse("N/A");
        
        // Get category summaries
        Map<String, Long> countsByCategory = loadAnalyticsPort.loadAnalyticsCountsByCategory();
        List<AnalyticsSummary.CategorySummary> categorySummaries = countsByCategory.entrySet().stream()
                .map(entry -> {
                    double percentage = totalAnalytics > 0 ? 
                        (double) entry.getValue() / totalAnalytics * 100 : 0.0;
                    return new AnalyticsSummary.CategorySummary(
                            entry.getKey(), 
                            entry.getValue(), 
                            percentage
                    );
                })
                .collect(Collectors.toList());

        return new AnalyticsSummary(
                totalAnalytics,
                uniqueTypes,
                latestDate,
                oldestDate,
                categorySummaries
        );
    }
} 