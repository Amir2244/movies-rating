package org.hiast.analyticsapi.application.port.in;

import org.hiast.analyticsapi.domain.model.AnalyticsQuery;
import org.hiast.model.DataAnalytics;

import java.util.List;
import java.util.Optional;

/**
 * Input port (Use Case) for retrieving analytics data.
 * Defines the contract for analytics retrieval operations.
 */
public interface GetAnalyticsUseCase {

    /**
     * Retrieves analytics data based on query criteria.
     *
     * @param query The analytics query with filtering criteria
     * @return A paginated result containing analytics data
     */
    AnalyticsResult getAnalytics(AnalyticsQuery query);

    /**
     * Retrieves a specific analytics record by its ID.
     *
     * @param analyticsId The unique identifier of the analytics record
     * @return The analytics record if found, empty otherwise
     */
    Optional<DataAnalytics> getAnalyticsById(String analyticsId);

    /**
     * Retrieves all available analytics types.
     *
     * @return List of analytics types with their descriptions
     */
    List<AnalyticsTypeInfo> getAvailableAnalyticsTypes();

    /**
     * Retrieves analytics summary statistics.
     *
     * @return Summary statistics about the analytics data
     */
    AnalyticsSummary getAnalyticsSummary();

    /**
     * Represents the result of an analytics query with pagination information.
     */
    class AnalyticsResult {
        private final List<DataAnalytics> analytics;
        private final long totalElements;
        private final int totalPages;
        private final int currentPage;
        private final int pageSize;

        public AnalyticsResult(List<DataAnalytics> analytics, long totalElements, 
                              int totalPages, int currentPage, int pageSize) {
            this.analytics = analytics;
            this.totalElements = totalElements;
            this.totalPages = totalPages;
            this.currentPage = currentPage;
            this.pageSize = pageSize;
        }

        public List<DataAnalytics> getAnalytics() {
            return analytics;
        }

        public long getTotalElements() {
            return totalElements;
        }

        public int getTotalPages() {
            return totalPages;
        }

        public int getCurrentPage() {
            return currentPage;
        }

        public int getPageSize() {
            return pageSize;
        }

        public boolean hasNext() {
            return currentPage < totalPages - 1;
        }

        public boolean hasPrevious() {
            return currentPage > 0;
        }
    }

    /**
     * Represents information about an analytics type.
     */
    class AnalyticsTypeInfo {
        private final String type;
        private final String category;
        private final String description;
        private final long count;

        public AnalyticsTypeInfo(String type, String category, String description, long count) {
            this.type = type;
            this.category = category;
            this.description = description;
            this.count = count;
        }

        public String getType() {
            return type;
        }

        public String getCategory() {
            return category;
        }

        public String getDescription() {
            return description;
        }

        public long getCount() {
            return count;
        }
    }

    /**
     * Represents summary statistics about analytics data.
     */
    class AnalyticsSummary {
        private final long totalAnalytics;
        private final long uniqueTypes;
        private final String latestAnalyticsDate;
        private final String oldestAnalyticsDate;
        private final List<CategorySummary> categorySummaries;

        public AnalyticsSummary(long totalAnalytics, long uniqueTypes, 
                               String latestAnalyticsDate, String oldestAnalyticsDate,
                               List<CategorySummary> categorySummaries) {
            this.totalAnalytics = totalAnalytics;
            this.uniqueTypes = uniqueTypes;
            this.latestAnalyticsDate = latestAnalyticsDate;
            this.oldestAnalyticsDate = oldestAnalyticsDate;
            this.categorySummaries = categorySummaries;
        }

        public long getTotalAnalytics() {
            return totalAnalytics;
        }

        public long getUniqueTypes() {
            return uniqueTypes;
        }

        public String getLatestAnalyticsDate() {
            return latestAnalyticsDate;
        }

        public String getOldestAnalyticsDate() {
            return oldestAnalyticsDate;
        }

        public List<CategorySummary> getCategorySummaries() {
            return categorySummaries;
        }

        public static class CategorySummary {
            private final String category;
            private final long count;
            private final double percentage;

            public CategorySummary(String category, long count, double percentage) {
                this.category = category;
                this.count = count;
                this.percentage = percentage;
            }

            public String getCategory() {
                return category;
            }

            public long getCount() {
                return count;
            }

            public double getPercentage() {
                return percentage;
            }
        }
    }
} 