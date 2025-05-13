package org.hiast.batch.application.port.out;


import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.hiast.batch.domain.model.ProcessedRating;

/**
 * Output Port defining how to load and preprocess rating data.
 * Implementations (adapters) will handle specific data sources like HDFS/GCS.
 */
public interface RatingDataProviderPort {
    /**
     * Loads raw ratings data from the configured input source.
     *
     * @param spark The active SparkSession.
     * @return A Spark Dataset of Rows representing the raw ratings.
     */
    Dataset<Row> loadRawRatings(SparkSession spark);

    /**
     * Preprocesses the raw ratings Dataset into a Dataset of ProcessedRating.
     *
     * @param spark             The active SparkSession.
     * @param rawRatingsDataset The raw ratings Dataset.
     * @return A Spark Dataset of ProcessedRating objects, ready for training.
     */
    Dataset<ProcessedRating> preprocessRatings(SparkSession spark, Dataset<Row> rawRatingsDataset);
}