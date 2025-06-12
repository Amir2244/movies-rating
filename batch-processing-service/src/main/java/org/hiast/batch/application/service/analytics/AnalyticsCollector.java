package org.hiast.batch.application.service.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hiast.batch.domain.exception.AnalyticsCollectionException;
import org.hiast.model.DataAnalytics;

import java.util.List;

/**
 * Interface for analytics collectors following the Strategy pattern.
 * Each implementation handles a specific type of analytics collection.
 * 
 * This interface is part of the domain layer and defines the contract
 * for analytics collection without depending on external frameworks.
 */
public interface AnalyticsCollector {
    
    /**
     * Collects analytics data and returns a DataAnalytics object.
     *
     * @param ratingsDf The ratings dataset
     * @param moviesData The movies metadata dataset (can be null)
     * @param tagsData The tags dataset (can be null)
     * @return DataAnalytics object containing the collected metrics
     * @throws AnalyticsCollectionException if analytics collection fails
     */
    List<DataAnalytics> collectAnalytics(Dataset<Row> ratingsDf,
                                         Dataset<Row> moviesData,
                                         Dataset<Row> tagsData);
    
    /**
     * Returns the analytics type this collector handles.
     * 
     * @return The analytics type
     */
    String getAnalyticsType();
    
    /**
     * Checks if this collector can process the given data.
     * 
     * @param ratingsDf The ratings dataset
     * @param moviesData The movies metadata dataset
     * @param tagsData The tags dataset
     * @return true if the collector can process the data, false otherwise
     */
    boolean canProcess(Dataset<Row> ratingsDf, Dataset<Row> moviesData, Dataset<Row> tagsData);
    
    /**
     * Returns the priority of this collector for execution ordering.
     * Lower numbers indicate higher priority.
     * 
     * @return The execution priority
     */
    default int getPriority() {
        return 100;
    }
}
