package org.hiast.analyticsapi.domain.model;

import org.hiast.model.AnalyticsType;

import java.time.Instant;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/**
 * Domain model representing a query for analytics data.
 * Encapsulates all the criteria that can be used to filter analytics.
 */
public class AnalyticsQuery {
    
    private final List<AnalyticsType> types;
    private final Instant fromDate;
    private final Instant toDate;
    private final String analyticsId;
    private final String descriptionKeyword;
    private final Integer page;
    private final Integer size;
    private final String sortBy;
    private final SortDirection sortDirection;

    private AnalyticsQuery(Builder builder) {
        this.types = builder.types;
        this.fromDate = builder.fromDate;
        this.toDate = builder.toDate;
        this.analyticsId = builder.analyticsId;
        this.descriptionKeyword = builder.descriptionKeyword;
        this.page = builder.page;
        this.size = builder.size;
        this.sortBy = builder.sortBy;
        this.sortDirection = builder.sortDirection;
    }

    public Optional<List<AnalyticsType>> getTypes() {
        return Optional.ofNullable(types);
    }

    public Optional<Instant> getFromDate() {
        return Optional.ofNullable(fromDate);
    }

    public Optional<Instant> getToDate() {
        return Optional.ofNullable(toDate);
    }

    public Optional<String> getAnalyticsId() {
        return Optional.ofNullable(analyticsId);
    }

    public Optional<String> getDescriptionKeyword() {
        return Optional.ofNullable(descriptionKeyword);
    }

    public int getPage() {
        return page != null ? page : 0;
    }

    public int getSize() {
        return size != null ? size : 20;
    }

    public String getSortBy() {
        return sortBy != null ? sortBy : "generatedAt";
    }

    public SortDirection getSortDirection() {
        return sortDirection != null ? sortDirection : SortDirection.DESC;
    }

    /**
     * Checks if the query has any date range filters.
     */
    public boolean hasDateRange() {
        return fromDate != null || toDate != null;
    }

    /**
     * Checks if the query has type filters.
     */
    public boolean hasTypeFilters() {
        return types != null && !types.isEmpty();
    }

    /**
     * Creates a new builder for AnalyticsQuery.
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        private List<AnalyticsType> types;
        private Instant fromDate;
        private Instant toDate;
        private String analyticsId;
        private String descriptionKeyword;
        private Integer page;
        private Integer size;
        private String sortBy;
        private SortDirection sortDirection;

        public Builder withTypes(List<AnalyticsType> types) {
            this.types = types;
            return this;
        }

        public Builder withDateRange(Instant fromDate, Instant toDate) {
            this.fromDate = fromDate;
            this.toDate = toDate;
            return this;
        }

        public Builder withFromDate(Instant fromDate) {
            this.fromDate = fromDate;
            return this;
        }

        public Builder withToDate(Instant toDate) {
            this.toDate = toDate;
            return this;
        }

        public Builder withAnalyticsId(String analyticsId) {
            this.analyticsId = analyticsId;
            return this;
        }

        public Builder withDescriptionKeyword(String descriptionKeyword) {
            this.descriptionKeyword = descriptionKeyword;
            return this;
        }

        public Builder withPagination(int page, int size) {
            this.page = page;
            this.size = size;
            return this;
        }

        public Builder withSorting(String sortBy, SortDirection sortDirection) {
            this.sortBy = sortBy;
            this.sortDirection = sortDirection;
            return this;
        }

        public AnalyticsQuery build() {
            return new AnalyticsQuery(this);
        }
    }

    public enum SortDirection {
        ASC, DESC
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        AnalyticsQuery that = (AnalyticsQuery) o;
        return Objects.equals(types, that.types) &&
                Objects.equals(fromDate, that.fromDate) &&
                Objects.equals(toDate, that.toDate) &&
                Objects.equals(analyticsId, that.analyticsId) &&
                Objects.equals(descriptionKeyword, that.descriptionKeyword) &&
                Objects.equals(page, that.page) &&
                Objects.equals(size, that.size) &&
                Objects.equals(sortBy, that.sortBy) &&
                sortDirection == that.sortDirection;
    }

    @Override
    public int hashCode() {
        return Objects.hash(types, fromDate, toDate, analyticsId, descriptionKeyword, 
                           page, size, sortBy, sortDirection);
    }

    @Override
    public String toString() {
        return "AnalyticsQuery{" +
                "types=" + types +
                ", fromDate=" + fromDate +
                ", toDate=" + toDate +
                ", analyticsId='" + analyticsId + '\'' +
                ", descriptionKeyword='" + descriptionKeyword + '\'' +
                ", page=" + page +
                ", size=" + size +
                ", sortBy='" + sortBy + '\'' +
                ", sortDirection=" + sortDirection +
                '}';
    }
} 