package org.hiast.batch.application.pipeline;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hiast.batch.domain.model.ProcessedRating;

/**
 * Abstract base class for all pipeline contexts.
 * Contains common data and state shared between training and analytics pipelines.
 * This eliminates code duplication and provides a clean inheritance hierarchy.
 */
public abstract class BasePipelineContext {
    
    protected final SparkSession spark;
    
    // Common data loading state
    protected Dataset<Row> rawRatings;
    protected Dataset<Row> rawMovies;
    protected Dataset<Row> rawTags;
    protected Dataset<Row> rawLinks;
    
    // Common processed data state
    protected Dataset<ProcessedRating> processedRatings;
    protected Dataset<Row> ratingsDf; // For analytics and training processing
    
    // Common pipeline state flags
    protected boolean dataLoaded;
    protected boolean movieDataLoaded;
    protected boolean dataPreprocessed;

    public BasePipelineContext(SparkSession spark) {
        this.spark = spark;
        
        // Initialize common state flags
        this.dataLoaded = false;
        this.movieDataLoaded = false;
        this.dataPreprocessed = false;
    }

    // Common Spark Session
    public SparkSession getSpark() {
        return spark;
    }

    // Common Raw Data Getters/Setters
    public Dataset<Row> getRawRatings() {
        return rawRatings;
    }

    public void setRawRatings(Dataset<Row> rawRatings) {
        this.rawRatings = rawRatings;
    }

    public Dataset<Row> getRawMovies() {
        return rawMovies;
    }

    public void setRawMovies(Dataset<Row> rawMovies) {
        this.rawMovies = rawMovies;
    }

    public Dataset<Row> getRawTags() {
        return rawTags;
    }

    public void setRawTags(Dataset<Row> rawTags) {
        this.rawTags = rawTags;
    }

    public Dataset<Row> getRawLinks() {
        return rawLinks;
    }

    public void setRawLinks(Dataset<Row> rawLinks) {
        this.rawLinks = rawLinks;
    }

    // Common Processed Data Getters/Setters
    public Dataset<ProcessedRating> getProcessedRatings() {
        return processedRatings;
    }

    public void setProcessedRatings(Dataset<ProcessedRating> processedRatings) {
        this.processedRatings = processedRatings;
    }

    public Dataset<Row> getRatingsDf() {
        return ratingsDf;
    }

    public void setRatingsDf(Dataset<Row> ratingsDf) {
        this.ratingsDf = ratingsDf;
    }

    // Common State Management
    public boolean isDataLoaded() {
        return dataLoaded;
    }

    public void setDataLoaded(boolean dataLoaded) {
        this.dataLoaded = dataLoaded;
    }

    public boolean isMovieDataLoaded() {
        return movieDataLoaded;
    }

    public void setMovieDataLoaded(boolean movieDataLoaded) {
        this.movieDataLoaded = movieDataLoaded;
    }

    public boolean isDataPreprocessed() {
        return dataPreprocessed;
    }

    public void setDataPreprocessed(boolean dataPreprocessed) {
        this.dataPreprocessed = dataPreprocessed;
    }

    // Common Memory Management Helpers
    public void cleanupRawData() {
        if (rawRatings != null && rawRatings.storageLevel().useMemory()) {
            rawRatings.unpersist();
        }
        if (rawMovies != null && rawMovies.storageLevel().useMemory()) {
            rawMovies.unpersist();
        }
        if (rawTags != null && rawTags.storageLevel().useMemory()) {
            rawTags.unpersist();
        }
        if (rawLinks != null && rawLinks.storageLevel().useMemory()) {
            rawLinks.unpersist();
        }
    }

    public void cleanupProcessedData() {
        if (processedRatings != null && processedRatings.storageLevel().useMemory()) {
            processedRatings.unpersist();
        }
        if (ratingsDf != null && ratingsDf.storageLevel().useMemory()) {
            ratingsDf.unpersist();
        }
    }

    // Abstract method for pipeline-specific toString
    @Override
    public abstract String toString();
}
