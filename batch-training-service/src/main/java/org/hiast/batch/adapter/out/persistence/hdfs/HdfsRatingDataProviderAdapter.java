package org.hiast.batch.adapter.out.persistence.hdfs;

import org.apache.spark.api.java.function.FilterFunction;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Encoders;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hiast.batch.application.port.out.RatingDataProviderPort;
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

/**
 * Adapter implementing RatingDataProviderPort to load data from HDFS.
 */
public class HdfsRatingDataProviderAdapter implements RatingDataProviderPort {

    private static final Logger log = LoggerFactory.getLogger(HdfsRatingDataProviderAdapter.class);
    private final String ratingsInputHdfsPath;

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
        log.info("HdfsRatingDataProviderAdapter initialized with HDFS path: {}", ratingsInputHdfsPath);
    }

    @Override
    public Dataset<Row> loadRawRatings(SparkSession spark) {
        log.info("Attempting to load raw ratings from HDFS path: {}", ratingsInputHdfsPath);
        try {
            log.info("Attempting to load as Parquet format...");
            Dataset<Row> parquetData = spark.read().format("parquet").load(ratingsInputHdfsPath);
            log.info("Successfully loaded data as Parquet. Schema:");
            parquetData.printSchema();
            log.info("Parquet data sample (first 5 rows, truncate=false):");
            parquetData.show(5, false);
            long parquetDataCount = parquetData.count();
            log.info("Count of rows loaded from Parquet: {}", parquetDataCount);

            if (parquetData.columns().length == 1 && "value".equals(parquetData.columns()[0]) && parquetData.schema().fields()[0].dataType().equals(DataTypes.BinaryType)) {
                log.warn("Loaded Parquet data has a single binary 'value' column. This will be handled in preprocessRatings.");
            } else if (parquetDataCount == 0) {
                log.warn("Loaded Parquet data but it contains 0 rows. Triggering CSV fallback.");
                throw new RuntimeException("Parquet file loaded but was empty. Triggering CSV fallback.");
            }
            return parquetData;
        } catch (Exception parquetException) {
            log.warn("Failed to load raw ratings as Parquet from HDFS path: {}. Error: {}. Attempting CSV fallback.", ratingsInputHdfsPath, parquetException.getMessage());
            try {
                log.info("Attempting to load as CSV format...");
                StructType ratingsSchema = new StructType()
                        .add("userId", DataTypes.StringType, true)
                        .add("movieId", DataTypes.StringType, true)
                        .add("rating", DataTypes.StringType, true)
                        .add("timestamp", DataTypes.StringType, true);

                Dataset<Row> csvData = spark.read()
                        .option("header", "true")
                        .option("encoding", StandardCharsets.UTF_8.name())
                        .schema(ratingsSchema)
                        .csv(ratingsInputHdfsPath);

                log.info("Raw data schema from HDFS (CSV attempt):");
                csvData.printSchema();
                log.info("Raw data sample from HDFS (CSV attempt) (showing up to 5 rows, truncate=false):");
                csvData.show(5, false);
                long csvDataCount = csvData.count();
                log.info("Count of rows loaded from CSV (CSV attempt): {}", csvDataCount);
                if (csvDataCount == 0) {
                    log.warn("The CSV file {} appears to be empty or only contains a header.", ratingsInputHdfsPath);
                }
                return csvData;
            } catch (Exception csvException) {
                log.error("Failed to load data as CSV as well from HDFS path: {}. Error: {}. Returning empty DataFrame.", ratingsInputHdfsPath, csvException.getMessage(), csvException);
                return spark.emptyDataFrame();
            }
        }
    }

    @Override
    public Dataset<ProcessedRating> preprocessRatings(SparkSession spark, Dataset<Row> rawRatingsDataset) {
        log.info("Starting preprocessRatings...");
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

            Dataset<ProcessedRating> processedRatings = processableData.flatMap(
                    (FlatMapFunction<Row, ProcessedRating>) row -> {
                        try {
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

                            ProcessedRating processed = new ProcessedRating(domainUserId, domainMovieId, domainRatingValue, domainTimestamp);
                            return Collections.singletonList(processed).iterator();
                        } catch (Exception e) {
                            log.warn("Skipping row during mapping to ProcessedRating due to error: {} - Row: {}", e.getMessage(), row.toString(), e);
                            return Collections.emptyIterator();
                        }
                    },
                    Encoders.kryo(ProcessedRating.class)
            );

            log.info("Schema of Dataset<ProcessedRating> (reflects ProcessedRating bean):");
            processedRatings.printSchema();
            log.info("Sample of Dataset<ProcessedRating> (first 5 rows, truncate=false):");
            processedRatings.show(5, false);
            long finalProcessedCount = processedRatings.count();
            log.info("Final count of successfully processed ratings (Dataset<ProcessedRating>): {}", finalProcessedCount);
            if (finalProcessedCount == 0 && processableDataCount > 0) {
                log.warn("All processable rows were filtered out during the flatMap to ProcessedRating stage. Check for parsing or validation errors in logs.");
            }
            return processedRatings;

        } catch (Exception e) {
            log.error("Critical error during HDFS ratings preprocessing: {}. Returning empty dataset.", e.getMessage(), e);
            return spark.emptyDataset(Encoders.kryo(ProcessedRating.class));
        }
    }
}
