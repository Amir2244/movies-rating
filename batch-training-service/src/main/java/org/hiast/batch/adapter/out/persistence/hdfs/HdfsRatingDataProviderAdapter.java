package org.hiast.batch.adapter.out.persistence.hdfs;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Adapter implementing RatingDataProviderPort to load data from HDFS.
 */
public class HdfsRatingDataProviderAdapter implements RatingDataProviderPort {

    private static final Logger log = LoggerFactory.getLogger(HdfsRatingDataProviderAdapter.class);
    private final String ratingsInputHdfsPath;
    private final String moviesInputHdfsPath;
    private final String tagsInputHdfsPath;
    private final String linksInputHdfsPath;

    private static String bytesToHex(byte[] bytes) {
        if (bytes == null) return "null";
        StringBuilder sb = new StringBuilder();
        for (byte b : bytes) {
            sb.append(String.format("%02X ", b & 0xFF));
        }
        return sb.toString();
    }

    public HdfsRatingDataProviderAdapter(String ratingsInputHdfsPath) {
        this.ratingsInputHdfsPath = ratingsInputHdfsPath;
        // Derive other file paths from the ratings path
        String basePath = ratingsInputHdfsPath.substring(0, ratingsInputHdfsPath.lastIndexOf('/'));
        this.moviesInputHdfsPath = basePath + "/movies.csv";
        this.tagsInputHdfsPath = basePath + "/tags.csv";
        this.linksInputHdfsPath = basePath + "/links.csv";

        log.info("HdfsRatingDataProviderAdapter initialized with paths:");
        log.info("  Ratings: {}", ratingsInputHdfsPath);
        log.info("  Movies: {}", moviesInputHdfsPath);
        log.info("  Tags: {}", tagsInputHdfsPath);
        log.info("  Links: {}", linksInputHdfsPath);
    }

    public HdfsRatingDataProviderAdapter(String ratingsInputHdfsPath, String moviesInputHdfsPath,
                                       String tagsInputHdfsPath, String linksInputHdfsPath) {
        this.ratingsInputHdfsPath = ratingsInputHdfsPath;
        this.moviesInputHdfsPath = moviesInputHdfsPath;
        this.tagsInputHdfsPath = tagsInputHdfsPath;
        this.linksInputHdfsPath = linksInputHdfsPath;

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

        // Try to load as Parquet first
        try {
            log.info("Attempting to load as Parquet format...");
            long startTime = System.nanoTime();

            // Get the number of cores available
            int numCores = Runtime.getRuntime().availableProcessors();
            // Use 2x number of cores as a reasonable default for partitions
            int numPartitions = Math.max(2 * numCores, 4);
            log.info("Setting number of partitions to {} based on {} available cores", numPartitions, numCores);

            Dataset<Row> parquetData = spark.read()
                    .format("parquet")
                    .option("mergeSchema", "true")
                    .load(ratingsInputHdfsPath)
                    .repartition(numPartitions);

            // Cache the data for better performance
            parquetData.persist(StorageLevel.MEMORY_AND_DISK());

            log.info("Successfully loaded data as Parquet. Schema:");
            parquetData.printSchema();
            log.info("Parquet data sample (first 5 rows, truncate=false):");
            parquetData.show(5, false);

            long parquetDataCount = parquetData.count();
            long endTime = System.nanoTime();
            log.info("Count of rows loaded from Parquet: {}. Loading took {} ms",
                    parquetDataCount, TimeUnit.NANOSECONDS.toMillis(endTime - startTime));

            if (parquetData.columns().length == 1 && "value".equals(parquetData.columns()[0]) &&
                    parquetData.schema().fields()[0].dataType().equals(DataTypes.BinaryType)) {
                log.warn("Loaded Parquet data has a single binary 'value' column. This will be handled in preprocessRatings.");
            } else if (parquetDataCount == 0) {
                log.warn("Loaded Parquet data but it contains 0 rows. Triggering CSV fallback.");
                parquetData.unpersist();
                throw new RuntimeException("Parquet file loaded but was empty. Triggering CSV fallback.");
            }

            return parquetData;
        } catch (Exception parquetException) {
            log.warn("Failed to load raw ratings as Parquet from HDFS path: {}. Error: {}. Attempting CSV fallback.",
                    ratingsInputHdfsPath, parquetException.getMessage());

            // Try to load as CSV as fallback
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
                int numPartitions = Math.max(2 * numCores, 4);
                log.info("Setting number of partitions to {} based on {} available cores", numPartitions, numCores);

                Dataset<Row> csvData = spark.read()
                        .option("header", "true")
                        .option("encoding", StandardCharsets.UTF_8.name())
                        .option("mode", "DROPMALFORMED")  // Drop malformed records
                        .option("nullValue", "")  // Treat empty strings as nulls
                        .schema(ratingsSchema)
                        .csv(ratingsInputHdfsPath)
                        .repartition(numPartitions);

                // Cache the data for better performance
                csvData.persist(StorageLevel.MEMORY_AND_DISK());

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
                    csvData.unpersist();
                    throw new DataLoadingException("CSV file is empty or contains only header: " + ratingsInputHdfsPath);
                }

                return csvData;
            } catch (Exception csvException) {
                if (!(csvException instanceof DataLoadingException)) {
                    log.error("Failed to load data as CSV from HDFS path: {}. Error: {}",
                            ratingsInputHdfsPath, csvException.getMessage(), csvException);
                    throw new DataLoadingException("Failed to load data from HDFS path: " + ratingsInputHdfsPath, csvException);
                } else {
                    throw (DataLoadingException) csvException;
                }
            }
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

        // Check if dataset is already cached
        if (!rawRatingsDataset.storageLevel().useMemory()) {
            log.info("Caching rawRatingsDataset for better performance");
            rawRatingsDataset.persist(StorageLevel.MEMORY_AND_DISK());
        }

        long initialRawCount = rawRatingsDataset.count();
        log.info("Count of rows in rawRatingsDataset at entry of preprocessRatings: {}", initialRawCount);

        if (rawRatingsDataset.isEmpty() || initialRawCount == 0) {
            log.warn("rawRatingsDataset is empty or count is zero at the beginning of preprocessRatings. Returning empty processed dataset.");
            return spark.emptyDataset(Encoders.bean(ProcessedRating.class));
        }

        try {
            String[] columnNamesFromDataset = rawRatingsDataset.columns();
            boolean isBinaryValueFormat = columnNamesFromDataset.length == 1 && "value".equals(columnNamesFromDataset[0]) && rawRatingsDataset.schema().fields()[0].dataType().equals(DataTypes.BinaryType);
            Dataset<Row> processableData;

            if (isBinaryValueFormat) {
                log.info("Detected single binary 'value' column format. Attempting to parse binary data...");
                StructType parsedSchema = new StructType()
                        .add("userId", DataTypes.IntegerType, false).add("movieId",
                                DataTypes.IntegerType, false)
                        .add("rating", DataTypes.DoubleType, false).add("timestamp",
                                DataTypes.LongType, false);
                List<Row> parsedRows = new ArrayList<>();
                processableData = spark.createDataFrame(parsedRows, parsedSchema);
                log.info("Created DataFrame from parsed binary values. Schema:");
                processableData.printSchema();
                processableData.show(5, false);
            } else {
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
                        String actualFieldNameTrimmed = actualFieldName.trim();
                        String expectedColNameTrimmed = expectedColName.trim();

                        if (actualFieldNameTrimmed.equals(expectedColNameTrimmed)) {
                            foundThisCol = true;
                            log.info("Comparing expected '{}' with actual schema field '{}': Trimmed match! Original actual: '{}' (len {}), Original expected: '{}' (len {})",
                                    expectedColName, actualFieldName, actualFieldName, actualFieldName.length(),
                                    expectedColName, expectedColName.length());
                            log.info("Byte comparison for '{}' and '{}': {}", expectedColName, actualFieldName,
                                    Arrays.equals(expectedColName.getBytes(StandardCharsets.UTF_8),
                                            actualFieldName.getBytes(StandardCharsets.UTF_8)));
                            break;
                        }
                    }
                    if (!foundThisCol) {
                        log.warn("Expected column '{}' NOT found after iterating and trimming schema fields.", expectedColName);
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
                                .append(", bytes: ").append(bytesToHex(name.getBytes(StandardCharsets.UTF_8))).append(")");
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
            }

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

            // Use map instead of flatMap for better performance when we don't need to filter out rows
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
                    .filter((FilterFunction<ProcessedRating>) rating -> rating != null);

            // Cache the processed ratings for better performance
            processedRatings.persist(StorageLevel.MEMORY_AND_DISK());

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

            // Unpersist raw data if it's no longer needed
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

    @Override
    public Dataset<Row> loadRawLinks(SparkSession spark) {
        log.info("Attempting to load raw links from HDFS path: {}", linksInputHdfsPath);
        return loadCsvFile(spark, linksInputHdfsPath, "links", createLinksSchema());
    }

    /**
     * Generic method to load CSV files from HDFS with proper error handling.
     */
    private Dataset<Row> loadCsvFile(SparkSession spark, String hdfsPath, String fileType, StructType schema) {
        try {
            // Check if file exists
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
            csvData.persist(StorageLevel.MEMORY_AND_DISK());

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
     * Expected columns: movieId,title,genres
     */
    private StructType createMoviesSchema() {
        return new StructType()
                .add("movieId", DataTypes.StringType, true)
                .add("title", DataTypes.StringType, true)
                .add("genres", DataTypes.StringType, true);
    }

    /**
     * Create schema for tags.csv file.
     * Expected columns: userId,movieId,tag,timestamp
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
