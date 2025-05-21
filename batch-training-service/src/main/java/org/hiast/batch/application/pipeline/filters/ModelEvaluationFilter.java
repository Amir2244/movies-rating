package org.hiast.batch.application.pipeline.filters;

import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hiast.batch.application.pipeline.Filter;
import org.hiast.batch.application.pipeline.ALSTrainingPipelineContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;

/**
 * Filter that evaluates the trained model using the test data.
 */
public class ModelEvaluationFilter implements Filter<ALSTrainingPipelineContext, ALSTrainingPipelineContext> {
    private static final Logger log = LoggerFactory.getLogger(ModelEvaluationFilter.class);

    @Override
    public ALSTrainingPipelineContext process(ALSTrainingPipelineContext context) {
        Dataset<Row> testData = context.getTestData();
        ALSModel model = context.getModel();

        if (testData == null || testData.isEmpty()) {
            log.warn("Test data is empty or null, skipping model evaluation.");
            return context;
        }

        log.info("Evaluating model on test data...");
        log.info("Schema of test data:");
        testData.printSchema();
        testData.show(5, false);

        String userCol = "userId";
        String itemCol = "movieId";
        String ratingCol = "ratingActual";

        if (testData.columns().length > 0) {
            boolean hasUserColTest = Arrays.asList(testData.columns()).contains(userCol);
            boolean hasItemColTest = Arrays.asList(testData.columns()).contains(itemCol);
            boolean hasRatingColTest = Arrays.asList(testData.columns()).contains(ratingCol);

            if (!hasUserColTest || !hasItemColTest || !hasRatingColTest) {
                log.error("Test DataFrame does not contain all required columns for model transformation. " +
                        "UserCol present: {}, ItemCol present: {}, RatingCol present: {}. Available: {}",
                        hasUserColTest, hasItemColTest, hasRatingColTest, String.join(", ", testData.columns()));
                return context;
            } else {
                Dataset<Row> predictions = model.transform(testData);
                log.info("Predictions sample:");
                predictions.show(5, false);

                RegressionEvaluator evaluator = new RegressionEvaluator()
                        .setMetricName("rmse")
                        .setLabelCol(ratingCol)
                        .setPredictionCol("prediction");
                double rmse = evaluator.evaluate(predictions);
                log.info("Model Evaluation - Root Mean Squared Error (RMSE) on test data = {}", rmse);

                // Set the evaluation metric in the context
                context.setEvaluationMetric(rmse);
            }
        } else {
            log.warn("Test DataFrame has no columns. Skipping evaluation.");
        }

        context.markModelEvaluationCompleted();
        return context;
    }
}