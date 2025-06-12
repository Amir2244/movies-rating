package org.hiast.analyticsapi.adapter.in.web;

import org.hiast.analyticsapi.application.port.in.GetAnalyticsUseCase;
import org.hiast.analyticsapi.domain.model.AnalyticsQuery;
import org.hiast.model.AnalyticsType;
import org.hiast.model.DataAnalytics;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.Max;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/**
 * REST controller for analytics endpoints.
 * Provides HTTP API for querying analytics data.
 */
@RestController
@RequestMapping("/api/v1/analytics")
@CrossOrigin(origins = "*")
public class AnalyticsController {

    private final GetAnalyticsUseCase getAnalyticsUseCase;

    public AnalyticsController(GetAnalyticsUseCase getAnalyticsUseCase) {
        this.getAnalyticsUseCase = getAnalyticsUseCase;
    }

    /**
     * Gets analytics data with filtering and pagination.
     * 
     * @param types Analytics types to filter by (optional)
     * @param fromDate Start date for filtering (optional)
     * @param toDate End date for filtering (optional)
     * @param keyword Keyword to search in descriptions (optional)
     * @param page Page number (default: 0)
     * @param size Page size (default: 20, max: 100)
     * @param sortBy Field to sort by (default: generatedAt)
     * @param sortDirection Sort direction (ASC/DESC, default: DESC)
     * @return Paginated analytics results
     */
    @GetMapping
    public ResponseEntity<GetAnalyticsUseCase.AnalyticsResult> getAnalytics(
            @RequestParam(required = false) List<String> types,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime fromDate,
            @RequestParam(required = false) @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) LocalDateTime toDate,
            @RequestParam(required = false) String keyword,
            @RequestParam(defaultValue = "0") @Min(0) int page,
            @RequestParam(defaultValue = "20") @Min(1) @Max(100) int size,
            @RequestParam(defaultValue = "generatedAt") String sortBy,
            @RequestParam(defaultValue = "DESC") String sortDirection) {

        AnalyticsQuery.Builder queryBuilder = AnalyticsQuery.builder()
                .withPagination(page, size)
                .withSorting(sortBy, AnalyticsQuery.SortDirection.valueOf(sortDirection.toUpperCase()));

        // Add type filters if provided
        if (types != null && !types.isEmpty()) {
            List<AnalyticsType> analyticsTypes = types.stream()
                    .map(type -> {
                        try {
                            return AnalyticsType.valueOf(type.toUpperCase());
                        } catch (IllegalArgumentException e) {
                            throw new IllegalArgumentException("Invalid analytics type: " + type);
                        }
                    })
                    .collect(Collectors.toList());
            queryBuilder.withTypes(analyticsTypes);
        }

        // Add date range filters if provided
        if (fromDate != null) {
            queryBuilder.withFromDate(fromDate.toInstant(ZoneOffset.UTC));
        }
        if (toDate != null) {
            queryBuilder.withToDate(toDate.toInstant(ZoneOffset.UTC));
        }

        // Add keyword filter if provided
        if (keyword != null && !keyword.trim().isEmpty()) {
            queryBuilder.withDescriptionKeyword(keyword.trim());
        }

        AnalyticsQuery query = queryBuilder.build();
        GetAnalyticsUseCase.AnalyticsResult result = getAnalyticsUseCase.getAnalytics(query);

        return ResponseEntity.ok(result);
    }

    /**
     * Gets a specific analytics record by ID.
     * 
     * @param analyticsId The analytics ID
     * @return The analytics record or 404 if not found
     */
    @GetMapping("/{analyticsId}")
    public ResponseEntity<DataAnalytics> getAnalyticsById(@PathVariable String analyticsId) {
        Optional<DataAnalytics> analytics = getAnalyticsUseCase.getAnalyticsById(analyticsId);
        return analytics.map(ResponseEntity::ok)
                       .orElse(ResponseEntity.notFound().build());
    }

    /**
     * Gets all available analytics types with their counts.
     * 
     * @return List of analytics types with metadata
     */
    @GetMapping("/types")
    public ResponseEntity<List<GetAnalyticsUseCase.AnalyticsTypeInfo>> getAnalyticsTypes() {
        List<GetAnalyticsUseCase.AnalyticsTypeInfo> types = getAnalyticsUseCase.getAvailableAnalyticsTypes();
        return ResponseEntity.ok(types);
    }

    /**
     * Gets summary statistics about analytics data.
     * 
     * @return Analytics summary with counts and statistics
     */
    @GetMapping("/summary")
    public ResponseEntity<GetAnalyticsUseCase.AnalyticsSummary> getAnalyticsSummary() {
        GetAnalyticsUseCase.AnalyticsSummary summary = getAnalyticsUseCase.getAnalyticsSummary();
        return ResponseEntity.ok(summary);
    }

    /**
     * Gets all available analytics type enums.
     * 
     * @return List of all possible analytics types
     */
    @GetMapping("/types/enum")
    public ResponseEntity<List<AnalyticsTypeResponse>> getAllAnalyticsTypeEnums() {
        List<AnalyticsTypeResponse> types = Arrays.stream(AnalyticsType.values())
                .map(type -> new AnalyticsTypeResponse(
                        type.name(),
                        type.getCategory(),
                        type.getDescription()
                ))
                .collect(Collectors.toList());
        return ResponseEntity.ok(types);
    }

    /**
     * Health check endpoint.
     */
    @GetMapping("/health")
    public ResponseEntity<HealthResponse> health() {
        return ResponseEntity.ok(new HealthResponse("OK", "Analytics API is running"));
    }

    // Response DTOs
    public static class AnalyticsTypeResponse {
        private final String name;
        private final String category;
        private final String description;

        public AnalyticsTypeResponse(String name, String category, String description) {
            this.name = name;
            this.category = category;
            this.description = description;
        }

        public String getName() {
            return name;
        }

        public String getCategory() {
            return category;
        }

        public String getDescription() {
            return description;
        }
    }

    public static class HealthResponse {
        private final String status;
        private final String message;

        public HealthResponse(String status, String message) {
            this.status = status;
            this.message = message;
        }

        public String getStatus() {
            return status;
        }

        public String getMessage() {
            return message;
        }
    }
} 