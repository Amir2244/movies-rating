package org.hiast.batch.launcher;

import org.hiast.batch.adapter.driver.http.HealthCheckServer;
import org.hiast.batch.adapter.driver.spark.BatchTrainingJob;
import org.hiast.batch.adapter.driver.spark.AnalyticsJob;
import org.hiast.batch.config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * Main launcher class that dispatches to different entry points based on command-line arguments.
 * This allows multiple entry points to be included in a single JAR file.
 *
 * Usage:
 * java -cp your-jar.jar org.hiast.batch.launcher.AppLauncher [train|predict]
 *
 * - train: Launches the BatchTrainingJob to train the model
 * - predict: Launches the ModelPredictionExample to test predictions with the trained model
 */
public class AppLauncher {
    private static final Logger log = LoggerFactory.getLogger(AppLauncher.class);

    public static void main(String[] args) {
        log.info("Starting Application Launcher...");

        if (args.length < 1) {
            printUsage();
            System.exit(1);
        }

        String command = args[0].toLowerCase();

        // Remove the first argument (command) to pass remaining args to the target main method
        String[] remainingArgs = new String[args.length - 1];
        System.arraycopy(args, 1, remainingArgs, 0, args.length - 1);

        switch (command) {
            case "train":
                log.info("Launching BatchTrainingJob...");
                BatchTrainingJob.main(remainingArgs);
                break;

            case "analytics":
                log.info("Launching AnalyticsJob...");
                AnalyticsJob.main(remainingArgs);
                break;

            case "health":
                log.info("Starting Health Check Server...");
                startHealthCheckServer(remainingArgs);
                break;

            default:
                log.error("Unknown command: {}", command);
                printUsage();
                System.exit(1);
        }
    }

    private static void printUsage() {
        System.out.println("Usage: java -cp your-jar.jar org.hiast.batch.launcher.AppLauncher [train|analytics|predict|health]");
        System.out.println("  train     - Launch the model training job");
        System.out.println("  analytics - Launch the dedicated analytics job (run before training)");
        System.out.println("  health    - Start the health check server for monitoring");
    }

    /**
     * Starts the health check server.
     *
     * @param args Command-line arguments
     */
    private static void startHealthCheckServer(String[] args) {
        int port = 8085; // Default port

        // Parse arguments
        for (String arg : args) {
            if (arg.startsWith("--port=")) {
                try {
                    port = Integer.parseInt(arg.substring("--port=".length()));
                } catch (NumberFormatException e) {
                    log.warn("Invalid port number: {}. Using default port 8080.", arg.substring("--port=".length()));
                }
            }
        }

        try {
            // Load configuration
            AppConfig appConfig = new AppConfig();

            // Create and start health check server
            HealthCheckServer server = new HealthCheckServer(port, appConfig);
            server.start();

            // Add shutdown hook to stop the server gracefully
            Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                log.info("Shutting down health check server...");
                server.stop();
            }));

            log.info("Health check server started on port {}. Press Ctrl+C to stop.", port);

            // Keep the main thread alive
            Thread.currentThread().join();
        } catch (IOException e) {
            log.error("Failed to start health check server: {}", e.getMessage(), e);
            System.exit(1);
        } catch (InterruptedException e) {
            log.info("Health check server interrupted.");
            Thread.currentThread().interrupt();
        }
    }
}