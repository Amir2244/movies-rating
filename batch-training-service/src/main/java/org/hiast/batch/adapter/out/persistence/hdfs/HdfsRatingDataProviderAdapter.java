package org.hiast.batch.adapter.out.persistence.hdfs;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;
import org.hiast.batch.application.port.out.RatingDataProviderPort;
import org.hiast.batch.domain.exception.DataLoadingException;
import org.hiast.batch.domain.model.ProcessedRating;
import org.hiast.ids.MovieId;
import org.hiast.ids.UserId;
import org.hiast.model.RatingValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

/**
 * Adapter implementing RatingDataProviderPort to load data from HDFS.
 */
public class HdfsRatingDataProviderAdapter implements RatingDataProviderPort {

    private static final Logger log = LoggerFactory.getLogger(HdfsRatingDataProviderAdapter.class);
    private final String ratingsInputHdfsPath;
    private final String moviesInputHdfsPath;
    private final String tagsInputHdfsPath;
   // private final String linksInputHdfsPath;

    public HdfsRatingDataProviderAdapter(String ratingsInputHdfsPath) {
        this.ratingsInputHdfsPath = ratingsInputHdfsPath;
        // Derive other file paths from the rating path
        String basePath = ratingsInputHdfsPath.substring(0, ratingsInputHdfsPath.lastIndexOf('/'));
        this.moviesInputHdfsPath = basePath + "/movies.csv";
        this.tagsInputHdfsPath = basePath + "/tags.csv";
       // this.linksInputHdfsPath = basePath + "/links.csv";

        log.info("HdfsRatingDataProviderAdapter initialized with paths:");
        log.info("  Ratings: {}", ratingsInputHdfsPath);
        log.info("  Movies: {}", moviesInputHdfsPath);
        log.info("  Tags: {}", tagsInputHdfsPath);
      //  log.info("  Links: {}", linksInputHdfsPath);
    }

    public HdfsRatingDataProviderAdapter(String ratingsInputHdfsPath, String moviesInputHdfsPath,
                                       String tagsInputHdfsPath, String linksInputHdfsPath) {
        this.ratingsInputHdfsPath = ratingsInputHdfsPath;
        this.moviesInputHdfsPath = moviesInputHdfsPath;
        this.tagsInputHdfsPath = tagsInputHdfsPath;
       // this.linksInputHdfsPath = linksInputHdfsPath;

        log.info("HdfsRatingDataProviderAdapter initialized with explicit paths:");
        log.info("  Ratings: {}", ratingsInputHdfsPath);
        log.info("  Movies: {}", moviesInputHdfsPath);
        log.info("  Tags: {}", tagsInputHdfsPath);
        log.info("  Links: {}", linksInputHdfsPath);
    }

    @Override
    public Dataset<Row> loadRawRatings(SparkSession spark) {
        log.info("Attempting to load raw ratings from HDFS path: {}", ratingsInputHdfsPath);

        // First, check if the file exists
        try {
            boolean fileExists = spark.sparkContext().hadoopConfiguration().get("fs.defaultFS") != null &&
                    new org.apache.hadoop.fs.Path(ratingsInputHdfsPath).getFileSystem(spark.sparkContext().hadoopConfiguration()).exists(new org.apache.hadoop.fs.Path(ratingsInputHdfsPath));

            if (!fileExists) {
                log.error("File does not exist at path: {}", ratingsInputHdfsPath);
                throw new DataLoadingException("File does not exist at path: " + ratingsInputHdfsPath);
            }
        } catch (Exception e) {
            if (!(e instanceof DataLoadingException)) {
                log.error("Error checking if file exists: {}", e.getMessage(), e);
                throw new DataLoadingException("Error checking if file exists: " + e.getMessage(), e);
            } else {
                throw (DataLoadingException) e;
            }
        }

        // Removed Parquet loading attempt block
        // Directly attempt to load as CSV

        try {
            log.info("Attempting to load as CSV format...");
            long startTime = System.nanoTime();

            // Define schema for better performance and data validation
            StructType ratingsSchema = new StructType()
                    .add("userId", DataTypes.StringType, true)
                    .add("movieId", DataTypes.StringType, true)
                    .add("rating", DataTypes.StringType, true)
                    .add("timestamp", DataTypes.StringType, true);

            // Get the number of cores available
            int numCores = Runtime.getRuntime().availableProcessors();
            // Use 2x number of cores as a reasonable default for partitions
            int numPartitions = Math.max(2* numCores, 4);
            log.info("Setting number of partitions to {} based on {} available cores", numPartitions, numCores);

            Dataset<Row> csvData = spark.read()
                    .option("header", "true")
                    .option("encoding", StandardCharsets.UTF_8.name())
                    .option("mode", "DROPMALFORMED")  // Drop malformed records
                    .option("nullValue", "")  // Treat empty strings as nulls
                    .schema(ratingsSchema)
                    .csv(ratingsInputHdfsPath)
                    .repartition(numPartitions);


            log.info("Raw data schema from HDFS (CSV attempt):");
            csvData.printSchema();
            log.info("Raw data sample from HDFS (CSV attempt) (showing up to 5 rows, truncate=false):");
            csvData.show(5, false);

            long csvDataCount = csvData.count();
            long endTime = System.nanoTime();
            log.info("Count of rows loaded from CSV: {}. Loading took {} ms",
                    csvDataCount, TimeUnit.NANOSECONDS.toMillis(endTime - startTime));

            if (csvDataCount == 0) {
                log.warn("The CSV file {} appears to be empty or only contains a header.", ratingsInputHdfsPath);
                // csvData.unpersist(); // Unpersisting here might be premature if an empty dataframe is valid in some contexts.
                                     // Let the caller decide or handle it. If an exception is preferred:
                throw new DataLoadingException("CSV file is empty or contains only header: " + ratingsInputHdfsPath);
            }

            return csvData;
        } catch (Exception csvException) {
            // If it's already a DataLoadingException, rethrow it directly
            if (csvException instanceof DataLoadingException) {
                throw (DataLoadingException) csvException;
            }
            // Otherwise, wrap it
            log.error("Failed to load data as CSV from HDFS path: {}. Error: {}",
                    ratingsInputHdfsPath, csvException.getMessage(), csvException);
            throw new DataLoadingException("Failed to load data from HDFS path: " + ratingsInputHdfsPath, csvException);
        }
    }

    @Override
    public Dataset<ProcessedRating> preprocessRatings(SparkSession spark, Dataset<Row> rawRatingsDataset) {
        log.info("Starting preprocessRatings...");
        long startTime = System.nanoTime();

        log.info("Schema of rawRatingsDataset at entry of preprocessRatings:");
        rawRatingsDataset.printSchema();
        log.info("Sample of rawRatingsDataset at entry (first 5 rows, truncate=false):");
        rawRatingsDataset.show(5, false);

        long initialRawCount = rawRatingsDataset.count();
        log.info("Count of rows in rawRatingsDataset at entry of preprocessRatings: {}", initialRawCount);

        if (rawRatingsDataset.isEmpty() || initialRawCount == 0) {
            log.warn("rawRatingsDataset is empty or count is zero at the beginning of preprocessRatings. Returning empty processed dataset.");
            return spark.emptyDataset(Encoders.bean(ProcessedRating.class));
        }

        try {
            // Removed binary format check, directly process as structured data
            Dataset<Row> processableData;

            log.info("Processing structured data. Expected columns: userId, movieId, rating, timestamp.");
            final String userIdCol = "userId";
            final String movieIdCol = "movieId";
            final String ratingCol = "rating";
            final String timestampCol = "timestamp";
            List<String> expectedCols = Arrays.asList(userIdCol, movieIdCol, ratingCol, timestampCol);

            StructType currentSchema = rawRatingsDataset.schema();
            boolean allColumnsFound = true;

            log.info("--- Detailed Schema Field Analysis ---");
            for (String expectedColName : expectedCols) {
                boolean foundThisCol = false;
                for (StructField actualField : currentSchema.fields()) {
                    String actualFieldName = actualField.name();
                    String actualFieldNameTrimmed = actualFieldName.trim(); // Trim whitespace
                    String expectedColNameTrimmed = expectedColName.trim(); // Trim whitespace

                    // Case-insensitive comparison for column names
                    if (actualFieldNameTrimmed.equalsIgnoreCase(expectedColNameTrimmed)) {
                        foundThisCol = true;
                        log.info("Comparing expected '{}' with actual schema field '{}': Trimmed, case-insensitive match! Original actual: '{}', Original expected: '{}'",
                                expectedColName, actualFieldName, actualFieldName, expectedColName);
                        break;
                    }
                }
                if (!foundThisCol) {
                    log.warn("Expected column '{}' NOT found after iterating, trimming, and case-insensitive comparison of schema fields.", expectedColName);
                    allColumnsFound = false;
                }
            }
            log.info("--- End Detailed Schema Field Analysis ---");


            if (!allColumnsFound) {
                StringBuilder actualNamesForLog = new StringBuilder();
                List<String> actualFieldNamesList = Arrays.asList(currentSchema.fieldNames());
                for (int i = 0; i < actualFieldNamesList.size(); i++) {
                    String name = actualFieldNamesList.get(i);
                    actualNamesForLog.append("Field ").append(i).append(": '").append(name).append("' (len: ").append(name.length())
                            .append(")");
                    if (i < actualFieldNamesList.size() - 1) actualNamesForLog.append(", ");
                }
                log.error("CRITICAL: Not all expected columns found. Expected: {}. Actual schema fields: [{}]",
                        expectedCols, actualNamesForLog);
                return spark.emptyDataset(Encoders.bean(ProcessedRating.class));
            }

            log.info("All expected columns reported as found by detailed check. Proceeding with filter.");
            processableData = rawRatingsDataset.filter((FilterFunction<Row>) row -> {
                try {
                    return !row.isNullAt(row.fieldIndex(userIdCol)) &&
                            !row.isNullAt(row.fieldIndex(movieIdCol)) &&
                            !row.isNullAt(row.fieldIndex(ratingCol)) &&
                            !row.isNullAt(row.fieldIndex(timestampCol));
                } catch (IllegalArgumentException e) {
                    log.warn("Column name not found during null check filtering for row: {}, error: {}. This row will be filtered out.",
                            row, e.getMessage());
                    return false;
                }
            });

            log.info("Processable data schema after initial handling and filtering:");
            processableData.printSchema();
            log.info("Processable data sample (first 5 rows, truncate=false):");
            processableData.show(5, false);
            long processableDataCount = processableData.count();
            log.info("Count of processable rows before mapping to ProcessedRating: {}", processableDataCount);

            if (processableDataCount == 0) {
                log.warn("ProcessableData is empty before mapping to ProcessedRating. Returning empty dataset.");
                return spark.emptyDataset(Encoders.bean(ProcessedRating.class));
            }

            // Use a map instead of flatMap for better performance when we don't need to filter out rows
            Dataset<ProcessedRating> processedRatings = processableData
                    // First filter out rows with null values to avoid exceptions during mapping
                    .filter((FilterFunction<Row>) row -> {
                        try {
                            return row.get(row.fieldIndex("userId")) != null &&
                                   row.get(row.fieldIndex("movieId")) != null &&
                                   row.get(row.fieldIndex("rating")) != null &&
                                   row.get(row.fieldIndex("timestamp")) != null;
                        } catch (Exception e) {
                            log.warn("Error checking for nulls in row: {}", row.toString(), e);
                            return false;
                        }
                    })
                    // Then map valid rows to ProcessedRating objects
                    .map((MapFunction<Row, ProcessedRating>) row -> {
                        Object userIdObj = row.get(row.fieldIndex("userId"));
                        Object movieIdObj = row.get(row.fieldIndex("movieId"));
                        Object ratingObj = row.get(row.fieldIndex("rating"));
                        Object timestampObj = row.get(row.fieldIndex("timestamp"));

                        int userIdInt = Integer.parseInt(userIdObj.toString().trim());
                        int movieIdInt = Integer.parseInt(movieIdObj.toString().trim());
                        double ratingDouble = Double.parseDouble(ratingObj.toString().trim());
                        long timestampLong = Long.parseLong(timestampObj.toString().trim());

                        UserId domainUserId = UserId.of(userIdInt);
                        MovieId domainMovieId = MovieId.of(movieIdInt);
                        RatingValue domainRatingValue = RatingValue.of(ratingDouble);
                        Instant domainTimestamp = Instant.ofEpochSecond(timestampLong);

                        return new ProcessedRating(domainUserId, domainMovieId, domainRatingValue, domainTimestamp);
                    }, Encoders.kryo(ProcessedRating.class))
                    // Handle exceptions during mapping
                    .filter((FilterFunction<ProcessedRating>) Objects::nonNull);

            // Cache the processed ratings for better performance
            processedRatings.persist(StorageLevel.MEMORY_AND_DISK_SER());

            log.info("Schema of Dataset<ProcessedRating> (reflects ProcessedRating bean):");
            processedRatings.printSchema();
            log.info("Sample of Dataset<ProcessedRating> (first 5 rows, truncate=false):");
            processedRatings.show(5, false);
            long finalProcessedCount = processedRatings.count();
            long endTime = System.nanoTime();
            log.info("Final count of successfully processed ratings (Dataset<ProcessedRating>): {}. Processing took {} ms",
                    finalProcessedCount, TimeUnit.NANOSECONDS.toMillis(endTime - startTime));

            if (finalProcessedCount == 0 && processableDataCount > 0) {
                log.warn("All processable rows were filtered out during mapping to ProcessedRating. Check for parsing or validation errors in logs.");
                processedRatings.unpersist();
                throw new DataLoadingException("All processable rows were filtered out during preprocessing. Check data format.");
            }

            // Unpersist raw data if it's no longer necessary
            if (rawRatingsDataset.storageLevel().useMemory()) {
                log.info("Unpersisting raw ratings dataset to free up memory");
                rawRatingsDataset.unpersist();
            }

            return processedRatings;

        } catch (Exception e) {
            log.error("Critical error during HDFS ratings preprocessing: {}. Returning empty dataset.", e.getMessage(), e);
            if (!(e instanceof DataLoadingException)) {
                throw new DataLoadingException("Error preprocessing ratings data", e);
            } else {
                throw (DataLoadingException) e;
            }
        }
    }

    @Override
    public Dataset<Row> loadRawMovies(SparkSession spark) {
        log.info("Attempting to load raw movies from HDFS path: {}", moviesInputHdfsPath);
        return loadCsvFile(spark, moviesInputHdfsPath, "movies", createMoviesSchema());
    }

    @Override
    public Dataset<Row> loadRawTags(SparkSession spark) {
        log.info("Attempting to load raw tags from HDFS path: {}", tagsInputHdfsPath);
        return loadCsvFile(spark, tagsInputHdfsPath, "tags", createTagsSchema());
    }

//    @Override
//    public Dataset<Row> loadRawLinks(SparkSession spark) {
//        log.info("Attempting to load raw links from HDFS path: {}", linksInputHdfsPath);
//        return loadCsvFile(spark, linksInputHdfsPath, "links", createLinksSchema());
//    }

    /**
     * Generic method to load CSV files from HDFS with proper error handling.
     */
    private Dataset<Row> loadCsvFile(SparkSession spark, String hdfsPath, String fileType, StructType schema) {
        try {
            // Check if a file exists
            boolean fileExists = checkFileExists(spark, hdfsPath);
            if (!fileExists) {
                log.warn("{} file does not exist at path: {}. Returning empty dataset.", fileType, hdfsPath);
                return spark.emptyDataFrame();
            }

            log.info("Loading {} file as CSV format...", fileType);
            long startTime = System.nanoTime();

            // Get optimal number of partitions
            int numCores = Runtime.getRuntime().availableProcessors();
            int numPartitions = Math.max(2 * numCores, 4);

            Dataset<Row> csvData = spark.read()
                    .option("header", "true")
                    .option("encoding", StandardCharsets.UTF_8.name())
                    .option("mode", "DROPMALFORMED")  // Drop malformed records
                    .option("nullValue", "")  // Treat empty strings as nulls
                    .schema(schema)
                    .csv(hdfsPath)
                    .repartition(numPartitions);

            // Cache the data for better performance
            csvData.persist(StorageLevel.MEMORY_AND_DISK_SER());

            long dataCount = csvData.count();
            long endTime = System.nanoTime();

            log.info("{} data loaded successfully:", fileType);
            log.info("  Schema:");
            csvData.printSchema();
            log.info("  Sample (first 5 rows):");
            csvData.show(5, false);
            log.info("  Count: {} rows. Loading took {} ms",
                    dataCount, TimeUnit.NANOSECONDS.toMillis(endTime - startTime));

            if (dataCount == 0) {
                log.warn("The {} file {} appears to be empty or only contains a header.", fileType, hdfsPath);
                csvData.unpersist();
                return spark.emptyDataFrame();
            }

            return csvData;

        } catch (Exception e) {
            log.error("Failed to load {} file from HDFS path: {}. Error: {}. Returning empty dataset.",
                    fileType, hdfsPath, e.getMessage(), e);
            return spark.emptyDataFrame();
        }
    }

    /**
     * Check if a file exists in HDFS.
     */
    private boolean checkFileExists(SparkSession spark, String hdfsPath) {
        try {
            return spark.sparkContext().hadoopConfiguration().get("fs.defaultFS") != null &&
                    new org.apache.hadoop.fs.Path(hdfsPath).getFileSystem(spark.sparkContext().hadoopConfiguration())
                            .exists(new org.apache.hadoop.fs.Path(hdfsPath));
        } catch (Exception e) {
            log.warn("Error checking if file exists at {}: {}", hdfsPath, e.getMessage());
            return false;
        }
    }

    /**
     * Create schema for movies.csv file.
     * Expected columns: movieId, title,genres
     */
    private StructType createMoviesSchema() {
        return new StructType()
                .add("movieId", DataTypes.StringType, true)
                .add("title", DataTypes.StringType, true)
                .add("genres", DataTypes.StringType, true);
    }

    /**
     * Create schema for tags.csv file.
     * Expected columns: userId, movieId, tag,timestamp
     */
    private StructType createTagsSchema() {
        return new StructType()
                .add("userId", DataTypes.StringType, true)
                .add("movieId", DataTypes.StringType, true)
                .add("tag", DataTypes.StringType, true)
                .add("timestamp", DataTypes.StringType, true);
    }

    /**
     * Create schema for links.csv file.
     * Expected columns: movieId,imdbId,tmdbId
     */
    private StructType createLinksSchema() {
        return new StructType()
                .add("movieId", DataTypes.StringType, true)
                .add("imdbId", DataTypes.StringType, true)
                .add("tmdbId", DataTypes.StringType, true);
    }
}
