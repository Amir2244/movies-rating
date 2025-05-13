package org.hiast.batch.launcher;

import org.hiast.batch.adapter.driver.spark.BatchTrainingJob;
import org.hiast.batch.example.ModelPredictionExample;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
                
            case "predict":
                log.info("Launching ModelPredictionExample...");
                ModelPredictionExample.main(remainingArgs);
                break;
                
            default:
                log.error("Unknown command: {}", command);
                printUsage();
                System.exit(1);
        }
    }
    
    private static void printUsage() {
        System.out.println("Usage: java -cp your-jar.jar org.hiast.batch.launcher.AppLauncher [train|predict]");
        System.out.println("  train   - Launch the model training job");
        System.out.println("  predict - Launch the model prediction example");
    }
}