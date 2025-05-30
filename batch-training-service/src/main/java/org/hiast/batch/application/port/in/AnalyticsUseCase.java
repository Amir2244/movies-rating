package org.hiast.batch.application.port.in;

/**
 * Use case interface for analytics collection and processing.
 * Follows the same pattern as TrainingModelUseCase for consistency.
 */
public interface AnalyticsUseCase {
    
    /**
     * Executes the analytics collection pipeline.
     * This method orchestrates the entire analytics process including:
     * - Data loading from HDFS
     * - Data preprocessing 
     * - Analytics collection in batches
     * - Results persistence to MongoDB
     */
    void executeAnalyticsPipeline();
}
