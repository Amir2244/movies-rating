package org.hiast.batch.application.service.analytics;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Result object for analytics execution containing success/failure information.
 * <p>
 * This class provides detailed information about the analytics execution
 * including which analytics succeeded, failed, or were skipped.
 */
public class AnalyticsExecutionResult {

    private final List<AnalyticsResult> results;
    private final String message;

    private AnalyticsExecutionResult(List<AnalyticsResult> results, String message) {
        this.results = Collections.unmodifiableList(new ArrayList<>(results));
        this.message = message;
    }

    /**
     * Creates an empty result with a message.
     *
     * @param message The message explaining why the result is empty
     * @return An empty analytics execution result
     */
    public static AnalyticsExecutionResult empty(String message) {
        return new AnalyticsExecutionResult(new ArrayList<>(), message);
    }

    /**
     * Creates a new builder for constructing analytics execution results.
     *
     * @return A new builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    /**
     * Gets all analytics results.
     *
     * @return Immutable list of analytics results
     */
    public List<AnalyticsResult> getResults() {
        return results;
    }

    /**
     * Gets the message associated with this result.
     *
     * @return The result message
     */
    public String getMessage() {
        return message;
    }

    /**
     * Gets the count of successful analytics.
     *
     * @return Number of successful analytics
     */
    public long getSuccessCount() {
        return results.stream().filter(r -> r.getStatus() == AnalyticsStatus.SUCCESS).count();
    }

    /**
     * Gets the count of failed analytics.
     *
     * @return Number of failed analytics
     */
    public long getFailureCount() {
        return results.stream().filter(r -> r.getStatus() == AnalyticsStatus.FAILURE).count();
    }

    /**
     * Gets the count of skipped analytics.
     *
     * @return Number of skipped analytics
     */
    public long getSkippedCount() {
        return results.stream().filter(r -> r.getStatus() == AnalyticsStatus.SKIPPED).count();
    }

    /**
     * Gets the total count of analytics.
     *
     * @return Total number of analytics
     */
    public int getTotalCount() {
        return results.size();
    }

    /**
     * Checks if all analytics were successful.
     *
     * @return true if all analytics succeeded, false otherwise
     */
    public boolean isAllSuccessful() {
        return !results.isEmpty() && getFailureCount() == 0 && getSkippedCount() == 0;
    }

    /**
     * Checks if any analytics failed.
     *
     * @return true if any analytics failed, false otherwise
     */
    public boolean hasFailures() {
        return getFailureCount() > 0;
    }

    /**
     * Gets successful analytics results.
     *
     * @return List of successful analytics results
     */
    public List<AnalyticsResult> getSuccessfulResults() {
        return results.stream()
                .filter(r -> r.getStatus() == AnalyticsStatus.SUCCESS)
                .collect(Collectors.toList());
    }

    /**
     * Gets failed analytics results.
     *
     * @return List of failed analytics results
     */
    public List<AnalyticsResult> getFailedResults() {
        return results.stream()
                .filter(r -> r.getStatus() == AnalyticsStatus.FAILURE)
                .collect(Collectors.toList());
    }

    /**
     * Builder class for constructing AnalyticsExecutionResult objects.
     */
    public static class Builder {
        private final List<AnalyticsResult> results = new ArrayList<>();
        private String message = "";

        /**
         * Adds a successful analytics result.
         *
         * @param analyticsType The type of analytics
         * @param analyticsId   The ID of the saved analytics
         * @return This builder for method chaining
         */
        public Builder addSuccess(String analyticsType, String analyticsId) {
            results.add(new AnalyticsResult(analyticsType, AnalyticsStatus.SUCCESS,
                    "Successfully collected and saved", analyticsId, null));
            return this;
        }

        /**
         * Adds a failed analytics result.
         *
         * @param analyticsType The type of analytics
         * @param errorMessage  The error message
         * @param exception     The exception that caused the failure (can be null)
         * @return This builder for method chaining
         */
        public Builder addFailure(String analyticsType, String errorMessage, Exception exception) {
            results.add(new AnalyticsResult(analyticsType, AnalyticsStatus.FAILURE,
                    errorMessage, null, exception));
            return this;
        }

        /**
         * Adds a skipped analytics result.
         *
         * @param analyticsType The type of analytics
         * @param reason        The reason for skipping
         * @return This builder for method chaining
         */
        public Builder addSkipped(String analyticsType, String reason) {
            results.add(new AnalyticsResult(analyticsType, AnalyticsStatus.SKIPPED,
                    reason, null, null));
            return this;
        }

        /**
         * Sets the overall message for the result.
         *
         * @param message The message
         * @return This builder for method chaining
         */
        public Builder withMessage(String message) {
            this.message = message;
            return this;
        }

        /**
         * Builds the analytics execution result.
         *
         * @return The constructed analytics execution result
         */
        public AnalyticsExecutionResult build() {
            if (message.isEmpty()) {
                message = String.format("Analytics execution completed: %d successful, %d failed, %d skipped",
                        results.stream().mapToInt(r -> r.getStatus() == AnalyticsStatus.SUCCESS ? 1 : 0).sum(),
                        results.stream().mapToInt(r -> r.getStatus() == AnalyticsStatus.FAILURE ? 1 : 0).sum(),
                        results.stream().mapToInt(r -> r.getStatus() == AnalyticsStatus.SKIPPED ? 1 : 0).sum());
            }
            return new AnalyticsExecutionResult(results, message);
        }
    }

    /**
     * Individual analytics result.
     */
    public static class AnalyticsResult {
        private final String analyticsType;
        private final AnalyticsStatus status;
        private final String message;
        private final String analyticsId;
        private final Exception exception;

        public AnalyticsResult(String analyticsType, AnalyticsStatus status, String message,
                               String analyticsId, Exception exception) {
            this.analyticsType = analyticsType;
            this.status = status;
            this.message = message;
            this.analyticsId = analyticsId;
            this.exception = exception;
        }

        public String getAnalyticsType() {
            return analyticsType;
        }

        public AnalyticsStatus getStatus() {
            return status;
        }

        public String getMessage() {
            return message;
        }

        public String getAnalyticsId() {
            return analyticsId;
        }

        public Exception getException() {
            return exception;
        }
    }

    /**
     * Status of analytics execution.
     */
    public enum AnalyticsStatus {
        SUCCESS, FAILURE, SKIPPED
    }
}
