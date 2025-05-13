package org.hiast.batch.example;

import org.apache.spark.SparkConf;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.hiast.batch.config.AppConfig;
import org.hiast.batch.config.HDFSConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Scanner;

/**
 * Enhanced example class demonstrating how to load a saved ALS model from HDFS
 * and use it to make predictions for new ratings.
 * 
 * This class provides a more robust way to test the model with examples:
 * 1. Can load test data from a file
 * 2. Supports command-line arguments for custom testing
 * 3. Provides detailed output and error handling
 * 
 * Usage:
 * - Basic: java -cp your-jar.jar org.hiast.batch.launcher.AppLauncher predict
 * - Custom model path: java -cp your-jar.jar org.hiast.batch.launcher.AppLauncher predict --model-path=hdfs://path/to/model
 * - Custom test data: java -cp your-jar.jar org.hiast.batch.launcher.AppLauncher predict --test-file=path/to/test/data.csv
 */
public class ModelPredictionExample {
    private static final Logger log = LoggerFactory.getLogger(ModelPredictionExample.class);

    public static void main(String[] args) {
        log.info("Initializing Enhanced Model Prediction Example...");

        // Parse command-line arguments
        String customModelPath = null;
        String testDataFile = null;

        for (String arg : args) {
            if (arg.startsWith("--model-path=")) {
                customModelPath = arg.substring("--model-path=".length());
                log.info("Using custom model path: {}", customModelPath);
            } else if (arg.startsWith("--test-file=")) {
                testDataFile = arg.substring("--test-file=".length());
                log.info("Using test data file: {}", testDataFile);
            }
        }

        // --- 1. Load Configuration ---
        AppConfig appConfig = new AppConfig();
        HDFSConfig hdfsConfig = appConfig.getHDFSConfig();
        String modelPath = (customModelPath != null) ? customModelPath : hdfsConfig.getModelSavePath();

        log.info("Model path: {}", modelPath);

        // --- 2. Initialize Spark Session with better configuration ---
        SparkConf sparkConf = new SparkConf()
                .setAppName("ALS-Model-Prediction-Example")
                .set("spark.executor.memory", "2g")
                .set("spark.driver.memory", "2g")
                .setMaster(appConfig.getSparkMasterUrl());

        SparkSession spark = SparkSession.builder().config(sparkConf).getOrCreate();
        log.info("SparkSession initialized. Spark version: {}", spark.version());
        log.info("Default FileSystem: {}", spark.sparkContext().hadoopConfiguration().get("fs.defaultFS"));

        try {
            // --- 3. Load the ALS model from HDFS ---
            log.info("Loading ALS model from HDFS at path: {}", modelPath);
            ALSModel loadedModel = ALSModel.load(modelPath);
            log.info("ALS model loaded successfully.");

            // Get model details
            log.info("Model rank: {}", loadedModel.rank());
            log.info("User factors count: {}", loadedModel.userFactors().count());
            log.info("Item factors count: {}", loadedModel.itemFactors().count());

            // --- 4. Create a dataset with user-movie pairs ---
            List<Row> testRatingsList;

            if (testDataFile != null) {
                // Load test data from file
                testRatingsList = loadTestDataFromFile(testDataFile);
                log.info("Loaded {} test ratings from file: {}", testRatingsList.size(), testDataFile);
            } else {
                // Use default test data
                testRatingsList = createDefaultTestData();
                log.info("Created {} default test ratings", testRatingsList.size());
            }

            // Define the schema for the test ratings
            StructType testRatingsSchema = new StructType(new StructField[]{
                    DataTypes.createStructField("userId", DataTypes.IntegerType, false),
                    DataTypes.createStructField("movieId", DataTypes.IntegerType, false)
            });

            // Create a DataFrame from the test ratings
            Dataset<Row> testRatingsDF = spark.createDataFrame(testRatingsList, testRatingsSchema);
            log.info("Created test ratings DataFrame:");
            testRatingsDF.show();

            // --- 5. Make predictions using the loaded model ---
            log.info("Making predictions for test ratings...");
            Dataset<Row> predictions = loadedModel.transform(testRatingsDF);

            // Show the predictions
            log.info("Predictions for test ratings:");
            predictions.select("userId", "movieId", "prediction").show();

            // Save predictions to a temporary location for further analysis
            String tempOutputPath = "hdfs://namenode:8020/tmp/predictions_" + System.currentTimeMillis();
            log.info("Saving predictions to: {}", tempOutputPath);
            predictions.select("userId", "movieId", "prediction")
                    .write()
                    .option("header", "true")
                    .csv(tempOutputPath);
            log.info("Predictions saved successfully.");

            log.info("Enhanced model prediction example completed successfully.");
        } catch (Exception e) {
            log.error("Error during model prediction: ", e);
            e.printStackTrace();
        } finally {
            // --- 6. Stop Spark Session ---
            log.info("Stopping SparkSession...");
            spark.stop();
            log.info("SparkSession stopped. Enhanced Model Prediction Example finished.");
        }
    }

    /**
     * Creates a default set of test data for model prediction.
     * 
     * @return List of Row objects containing userId and movieId pairs
     */
    private static List<Row> createDefaultTestData() {
        List<Row> testRatingsList = new ArrayList<>();

        // Add a variety of user-movie pairs for testing
        // userId, movieId (no rating needed for prediction)
        testRatingsList.add(RowFactory.create(1, 100));  // User 1, Movie 100
        testRatingsList.add(RowFactory.create(1, 200));  // User 1, Movie 200
        testRatingsList.add(RowFactory.create(1, 300));  // User 1, Movie 300
        testRatingsList.add(RowFactory.create(2, 100));  // User 2, Movie 100
        testRatingsList.add(RowFactory.create(2, 200));  // User 2, Movie 200
        testRatingsList.add(RowFactory.create(2, 300));  // User 2, Movie 300
        testRatingsList.add(RowFactory.create(3, 100));  // User 3, Movie 100
        testRatingsList.add(RowFactory.create(3, 200));  // User 3, Movie 200
        testRatingsList.add(RowFactory.create(3, 300));  // User 3, Movie 300

        // Add some edge cases
        testRatingsList.add(RowFactory.create(999, 999));  // User and movie that might not exist in training data
        testRatingsList.add(RowFactory.create(1, 999));    // Existing user, new movie
        testRatingsList.add(RowFactory.create(999, 100));  // New user, existing movie

        return testRatingsList;
    }

    /**
     * Loads test data from a CSV file.
     * Expected format: userId,movieId (one pair per line)
     * 
     * @param filePath Path to the CSV file
     * @return List of Row objects containing userId and movieId pairs
     * @throws FileNotFoundException if the file is not found
     */
    private static List<Row> loadTestDataFromFile(String filePath) throws FileNotFoundException {
        List<Row> testRatingsList = new ArrayList<>();

        try (Scanner scanner = new Scanner(new File(filePath))) {
            // Skip header if present
            if (scanner.hasNextLine()) {
                String firstLine = scanner.nextLine();
                if (!firstLine.matches("\\d+,\\d+")) {
                    // If the first line doesn't match the pattern of two numbers separated by a comma,
                    // assume it's a header and skip it
                    log.info("Skipping header line: {}", firstLine);
                } else {
                    // It's not a header, process it
                    String[] parts = firstLine.split(",");
                    if (parts.length >= 2) {
                        try {
                            int userId = Integer.parseInt(parts[0].trim());
                            int movieId = Integer.parseInt(parts[1].trim());
                            testRatingsList.add(RowFactory.create(userId, movieId));
                        } catch (NumberFormatException e) {
                            log.warn("Invalid data format in line: {}", firstLine);
                        }
                    }
                }
            }

            // Process remaining lines
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine().trim();
                if (!line.isEmpty()) {
                    String[] parts = line.split(",");
                    if (parts.length >= 2) {
                        try {
                            int userId = Integer.parseInt(parts[0].trim());
                            int movieId = Integer.parseInt(parts[1].trim());
                            testRatingsList.add(RowFactory.create(userId, movieId));
                        } catch (NumberFormatException e) {
                            log.warn("Invalid data format in line: {}", line);
                        }
                    }
                }
            }
        }

        if (testRatingsList.isEmpty()) {
            log.warn("No valid test data found in file: {}. Using default test data instead.", filePath);
            return createDefaultTestData();
        }

        return testRatingsList;
    }
}
