package org.hiast.batch.application.pipeline;

import org.apache.spark.sql.SparkSession;
import org.hiast.model.DataAnalytics;

import java.util.ArrayList;
import java.util.List;

/**
 * Pipeline context for analytics processing.
 * Extends BasePipelineContext to inherit common data and state management.
 * Adds analytics-specific state and functionality.
 */
public class AnalyticsPipelineContext extends BasePipelineContext {

    // Analytics-specific state
    private List<DataAnalytics> collectedAnalytics;
    private int successfulAnalyticsCount;
    private int failedAnalyticsCount;

    // Analytics pipeline state flags
    private boolean lightweightAnalyticsCompleted;
    private boolean mediumAnalyticsCompleted;
    private boolean heavyAnalyticsCompleted;
    private boolean analyticsCompleted;

    public AnalyticsPipelineContext(SparkSession spark) {
        super(spark); // Call parent constructor
        this.collectedAnalytics = new ArrayList<>();
        this.successfulAnalyticsCount = 0;
        this.failedAnalyticsCount = 0;

        // Initialize analytics-specific state flags
        this.lightweightAnalyticsCompleted = false;
        this.mediumAnalyticsCompleted = false;
        this.heavyAnalyticsCompleted = false;
        this.analyticsCompleted = false;
    }

    // Common getters/setters are inherited from BasePipelineContext

    // Analytics Results
    public List<DataAnalytics> getCollectedAnalytics() {
        return collectedAnalytics;
    }

    public void addAnalytics(DataAnalytics analytics) {
        this.collectedAnalytics.add(analytics);
        this.successfulAnalyticsCount++;
    }

    public void addAnalytics(List<DataAnalytics> analyticsList) {
        this.collectedAnalytics.addAll(analyticsList);
        this.successfulAnalyticsCount += analyticsList.size();
    }

    public void incrementFailedAnalytics() {
        this.failedAnalyticsCount++;
    }

    public int getSuccessfulAnalyticsCount() {
        return successfulAnalyticsCount;
    }

    public int getFailedAnalyticsCount() {
        return failedAnalyticsCount;
    }

    public int getTotalAnalyticsCount() {
        return successfulAnalyticsCount + failedAnalyticsCount;
    }

    // Common state management methods are inherited from BasePipelineContext

    // Analytics-specific state management

    public boolean isLightweightAnalyticsCompleted() {
        return lightweightAnalyticsCompleted;
    }

    public void setLightweightAnalyticsCompleted(boolean lightweightAnalyticsCompleted) {
        this.lightweightAnalyticsCompleted = lightweightAnalyticsCompleted;
    }

    public boolean isMediumAnalyticsCompleted() {
        return mediumAnalyticsCompleted;
    }

    public void setMediumAnalyticsCompleted(boolean mediumAnalyticsCompleted) {
        this.mediumAnalyticsCompleted = mediumAnalyticsCompleted;
    }

    public boolean isHeavyAnalyticsCompleted() {
        return heavyAnalyticsCompleted;
    }

    public void setHeavyAnalyticsCompleted(boolean heavyAnalyticsCompleted) {
        this.heavyAnalyticsCompleted = heavyAnalyticsCompleted;
    }

    public boolean isAnalyticsCompleted() {
        return analyticsCompleted;
    }

    public void setAnalyticsCompleted(boolean analyticsCompleted) {
        this.analyticsCompleted = analyticsCompleted;
    }

    // Helper methods for pipeline completion
    public void markAnalyticsCompleted() {
        this.analyticsCompleted = true;
    }

    @Override
    public String toString() {
        return "AnalyticsPipelineContext{" +
                "dataLoaded=" + dataLoaded +
                ", movieDataLoaded=" + movieDataLoaded +
                ", dataPreprocessed=" + dataPreprocessed +
                ", lightweightAnalyticsCompleted=" + lightweightAnalyticsCompleted +
                ", mediumAnalyticsCompleted=" + mediumAnalyticsCompleted +
                ", heavyAnalyticsCompleted=" + heavyAnalyticsCompleted +
                ", analyticsCompleted=" + analyticsCompleted +
                ", successfulAnalytics=" + successfulAnalyticsCount +
                ", failedAnalytics=" + failedAnalyticsCount +
                '}';
    }
}
