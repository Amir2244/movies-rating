package org.hiast.batch.domain.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.Map;
import java.util.Objects;

/**
 * Represents analytics data about model training and performance.
 * This is used to store useful metrics for analytics purposes.
 */
public class ModelAnalytics implements Serializable {
    private static final long serialVersionUID = 1L;

    private final String modelId;
    private final String modelVersion;
    private final Instant generatedAt;
    private final double evaluationMetric;
    private final Map<String, Object> metrics;
    private final Map<String, Object> parameters;
    private final long trainingTimeMs;
    private final int userCount;
    private final int itemCount;
    private final int ratingCount;

    /**
     * Constructor for ModelAnalytics.
     *
     * @param modelId        The unique identifier for the model.
     * @param modelVersion   The version of the model.
     * @param generatedAt    The timestamp when the analytics were generated.
     * @param evaluationMetric The primary evaluation metric (e.g., RMSE).
     * @param metrics        Additional metrics as key-value pairs.
     * @param parameters     Model parameters as key-value pairs.
     * @param trainingTimeMs The time taken to train the model in milliseconds.
     * @param userCount      The number of users in the dataset.
     * @param itemCount      The number of items in the dataset.
     * @param ratingCount    The number of ratings in the dataset.
     */
    public ModelAnalytics(String modelId, 
                         String modelVersion, 
                         Instant generatedAt, 
                         double evaluationMetric,
                         Map<String, Object> metrics,
                         Map<String, Object> parameters,
                         long trainingTimeMs,
                         int userCount,
                         int itemCount,
                         int ratingCount) {
        this.modelId = Objects.requireNonNull(modelId, "modelId cannot be null");
        this.modelVersion = Objects.requireNonNull(modelVersion, "modelVersion cannot be null");
        this.generatedAt = Objects.requireNonNull(generatedAt, "generatedAt cannot be null");
        this.evaluationMetric = evaluationMetric;
        this.metrics = Objects.requireNonNull(metrics, "metrics cannot be null");
        this.parameters = Objects.requireNonNull(parameters, "parameters cannot be null");
        this.trainingTimeMs = trainingTimeMs;
        this.userCount = userCount;
        this.itemCount = itemCount;
        this.ratingCount = ratingCount;
    }

    public String getModelId() {
        return modelId;
    }

    public String getModelVersion() {
        return modelVersion;
    }

    public Instant getGeneratedAt() {
        return generatedAt;
    }

    public double getEvaluationMetric() {
        return evaluationMetric;
    }

    public Map<String, Object> getMetrics() {
        return metrics;
    }

    public Map<String, Object> getParameters() {
        return parameters;
    }

    public long getTrainingTimeMs() {
        return trainingTimeMs;
    }

    public int getUserCount() {
        return userCount;
    }

    public int getItemCount() {
        return itemCount;
    }

    public int getRatingCount() {
        return ratingCount;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        ModelAnalytics that = (ModelAnalytics) o;
        return Double.compare(that.evaluationMetric, evaluationMetric) == 0 &&
                trainingTimeMs == that.trainingTimeMs &&
                userCount == that.userCount &&
                itemCount == that.itemCount &&
                ratingCount == that.ratingCount &&
                Objects.equals(modelId, that.modelId) &&
                Objects.equals(modelVersion, that.modelVersion) &&
                Objects.equals(generatedAt, that.generatedAt) &&
                Objects.equals(metrics, that.metrics) &&
                Objects.equals(parameters, that.parameters);
    }

    @Override
    public int hashCode() {
        return Objects.hash(modelId, modelVersion, generatedAt, evaluationMetric, metrics, parameters, 
                           trainingTimeMs, userCount, itemCount, ratingCount);
    }

    @Override
    public String toString() {
        return "ModelAnalytics{" +
                "modelId='" + modelId + '\'' +
                ", modelVersion='" + modelVersion + '\'' +
                ", generatedAt=" + generatedAt +
                ", evaluationMetric=" + evaluationMetric +
                ", metrics=" + metrics +
                ", parameters=" + parameters +
                ", trainingTimeMs=" + trainingTimeMs +
                ", userCount=" + userCount +
                ", itemCount=" + itemCount +
                ", ratingCount=" + ratingCount +
                '}';
    }
}
