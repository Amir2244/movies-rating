package org.hiast.batch.application.pipeline.filters;

import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hiast.batch.application.pipeline.Filter;
import org.hiast.batch.application.pipeline.ALSTrainingPipelineContext;
import org.hiast.batch.application.port.out.FactorCachingPort;
import org.hiast.batch.domain.exception.ModelPersistenceException;
import org.hiast.batch.domain.model.ModelFactors;
import org.hiast.ids.MovieId;
import org.hiast.ids.UserId;
import org.hiast.model.factors.ItemFactor;
import org.hiast.model.factors.UserFactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Filter that persists the model factors.
 */
public class FactorPersistenceFilter implements Filter<ALSTrainingPipelineContext, ALSTrainingPipelineContext> {
    private static final Logger log = LoggerFactory.getLogger(FactorPersistenceFilter.class);

    private final FactorCachingPort factorPersistence;

    public FactorPersistenceFilter(FactorCachingPort factorPersistence) {
        this.factorPersistence = factorPersistence;
    }

    @Override
    public ALSTrainingPipelineContext process(ALSTrainingPipelineContext context) {
        log.info("Persisting model factors...");

        ALSModel model = context.getModel();

        if (model == null) {
            log.error("Model is null. Cannot persist factors.");
            throw new ModelPersistenceException("Model is null. Cannot persist factors. Check if model training completed successfully.");
        }

        // Get Spark Dataset representations
        Dataset<Row> userFactorsDataset = model.userFactors().withColumnRenamed("id", "userId");
        Dataset<Row> itemFactorsDataset = model.itemFactors().withColumnRenamed("id", "itemId");

        // Create ModelFactors with Dataset representations
        ModelFactors modelFactors = new ModelFactors(userFactorsDataset, itemFactorsDataset);

        // Convert to generic factor models
        convertToGenericFactorModels(modelFactors);

        // Save the model factors
        factorPersistence.saveModelFactors(modelFactors);
        log.info("Model factors persisted successfully.");

        // Set the factors persisted flag in the context
        context.setFactorsPersisted(true);

        return context;
    }

    /**
     * Converts Spark Dataset representations to generic factor models.
     *
     * @param modelFactors The ModelFactors object containing Dataset representations
     */
    private void convertToGenericFactorModels(ModelFactors modelFactors) {
        if (!modelFactors.hasDatasetRepresentations()) {
            log.warn("ModelFactors does not have Dataset representations. Skipping conversion.");
            return;
        }

        log.info("Converting user factors Dataset to generic factor models...");
        List<UserFactor<float[]>> userFactors = convertUserFactors(modelFactors.getUserFactorsDataset());
        modelFactors.setUserFactors(userFactors);

        log.info("Converting item factors Dataset to generic factor models...");
        List<ItemFactor<float[]>> itemFactors = convertItemFactors(modelFactors.getItemFactorsDataset());
        modelFactors.setItemFactors(itemFactors);

        log.info("Conversion to generic factor models completed.");
    }

    /**
     * Converts a user factors Dataset to a list of UserFactor objects.
     *
     * @param userFactorsDataset The user factors Dataset
     * @return A list of UserFactor objects
     */
    private List<UserFactor<float[]>> convertUserFactors(Dataset<Row> userFactorsDataset) {
        if (userFactorsDataset == null || userFactorsDataset.isEmpty()) {
            log.warn("User factors Dataset is null or empty. Returning empty list.");
            return new ArrayList<>();
        }

        List<UserFactor<float[]>> result = new ArrayList<>();

        // Collect the Dataset to the driver (be careful with large datasets)
        List<Row> rows = userFactorsDataset.collectAsList();

        for (Row row : rows) {
            try {
                int userId = row.getInt(row.fieldIndex("userId"));
                Object featuresObj = row.get(row.fieldIndex("features"));
                float[] featureArray;

                // Handle different types of feature representations
                if (featuresObj instanceof Vector) {
                    // It's a Spark Vector
                    Vector features = (Vector) featuresObj;
                    featureArray = new float[features.size()];
                    for (int i = 0; i < features.size(); i++) {
                        featureArray[i] = (float) features.apply(i);
                    }
                } else if (featuresObj instanceof scala.collection.mutable.WrappedArray) {
                    // It's a Scala WrappedArray
                    scala.collection.mutable.WrappedArray<?> array =
                            (scala.collection.mutable.WrappedArray<?>) featuresObj;
                    featureArray = new float[array.length()];
                    for (int i = 0; i < array.length(); i++) {
                        Object elem = array.apply(i);
                        if (elem instanceof Double) {
                            featureArray[i] = ((Double) elem).floatValue();
                        } else if (elem instanceof Float) {
                            featureArray[i] = (Float) elem;
                        } else if (elem instanceof Integer) {
                            featureArray[i] = ((Integer) elem).floatValue();
                        } else {
                            featureArray[i] = Float.parseFloat(elem.toString());
                        }
                    }
                } else if (featuresObj instanceof java.util.List) {
                    // It's a Java List
                    java.util.List<?> list = (java.util.List<?>) featuresObj;
                    featureArray = new float[list.size()];
                    for (int i = 0; i < list.size(); i++) {
                        Object elem = list.get(i);
                        if (elem instanceof Double) {
                            featureArray[i] = ((Double) elem).floatValue();
                        } else if (elem instanceof Float) {
                            featureArray[i] = (Float) elem;
                        } else if (elem instanceof Integer) {
                            featureArray[i] = ((Integer) elem).floatValue();
                        } else {
                            featureArray[i] = Float.parseFloat(elem.toString());
                        }
                    }
                } else {
                    // Log the actual type for debugging
                    log.warn("Unexpected features type: {}", featuresObj.getClass().getName());
                    // Default to empty array
                    featureArray = new float[0];
                }

                // Create UserFactor object
                UserFactor<float[]> userFactor = new UserFactor<>(UserId.of(userId), featureArray);
                result.add(userFactor);
            } catch (Exception e) {
                log.error("Error converting user factor row: {}", e.getMessage(), e);
            }
        }

        log.info("Converted {} user factors to generic factor models", result.size());
        return result;
    }

    /**
     * Converts an item factors Dataset to a list of ItemFactor objects.
     *
     * @param itemFactorsDataset The item factors Dataset
     * @return A list of ItemFactor objects
     */
    private List<ItemFactor<float[]>> convertItemFactors(Dataset<Row> itemFactorsDataset) {
        if (itemFactorsDataset == null || itemFactorsDataset.isEmpty()) {
            log.warn("Item factors Dataset is null or empty. Returning empty list.");
            return new ArrayList<>();
        }

        List<ItemFactor<float[]>> result = new ArrayList<>();

        // Collect the Dataset to the driver (be careful with large datasets)
        List<Row> rows = itemFactorsDataset.collectAsList();

        for (Row row : rows) {
            try {
                int itemId = row.getInt(row.fieldIndex("itemId"));
                Object featuresObj = row.get(row.fieldIndex("features"));
                float[] featureArray;

                // Handle different types of feature representations
                if (featuresObj instanceof Vector) {
                    // It's a Spark Vector
                    Vector features = (Vector) featuresObj;
                    featureArray = new float[features.size()];
                    for (int i = 0; i < features.size(); i++) {
                        featureArray[i] = (float) features.apply(i);
                    }
                } else if (featuresObj instanceof scala.collection.mutable.WrappedArray) {
                    // It's a Scala WrappedArray
                    scala.collection.mutable.WrappedArray<?> array =
                            (scala.collection.mutable.WrappedArray<?>) featuresObj;
                    featureArray = new float[array.length()];
                    for (int i = 0; i < array.length(); i++) {
                        Object elem = array.apply(i);
                        if (elem instanceof Double) {
                            featureArray[i] = ((Double) elem).floatValue();
                        } else if (elem instanceof Float) {
                            featureArray[i] = (Float) elem;
                        } else if (elem instanceof Integer) {
                            featureArray[i] = ((Integer) elem).floatValue();
                        } else {
                            featureArray[i] = Float.parseFloat(elem.toString());
                        }
                    }
                } else if (featuresObj instanceof java.util.List) {
                    // It's a Java List
                    java.util.List<?> list = (java.util.List<?>) featuresObj;
                    featureArray = new float[list.size()];
                    for (int i = 0; i < list.size(); i++) {
                        Object elem = list.get(i);
                        if (elem instanceof Double) {
                            featureArray[i] = ((Double) elem).floatValue();
                        } else if (elem instanceof Float) {
                            featureArray[i] = (Float) elem;
                        } else if (elem instanceof Integer) {
                            featureArray[i] = ((Integer) elem).floatValue();
                        } else {
                            featureArray[i] = Float.parseFloat(elem.toString());
                        }
                    }
                } else {
                    // Log the actual type for debugging
                    log.warn("Unexpected features type: {}", featuresObj.getClass().getName());
                    // Default to empty array
                    featureArray = new float[0];
                }

                // Create ItemFactor object
                ItemFactor<float[]> itemFactor = new ItemFactor<>(MovieId.of(itemId), featureArray);
                result.add(itemFactor);
            } catch (Exception e) {
                log.error("Error converting item factor row: {}", e.getMessage(), e);
            }
        }

        log.info("Converted {} item factors to generic factor models", result.size());
        return result;
    }
}