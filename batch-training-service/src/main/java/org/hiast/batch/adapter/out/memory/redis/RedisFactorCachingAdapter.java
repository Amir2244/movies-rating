package org.hiast.batch.adapter.out.memory.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hiast.batch.application.port.out.FactorCachingPort;
import org.hiast.batch.domain.model.ModelFactors;
import org.hiast.ids.MovieId;
import org.hiast.ids.UserId;
import org.hiast.model.factors.ItemFactor;
import org.hiast.model.factors.UserFactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import scala.collection.Iterator;
import scala.collection.Seq;

public class RedisFactorCachingAdapter implements FactorCachingPort {

    private static final Logger log = LoggerFactory.getLogger(RedisFactorCachingAdapter.class);


    private final String redisHost;
    private final int redisPort;


    public RedisFactorCachingAdapter(String redisHost, int redisPort /*, String redisPassword */) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        log.info("RedisFactorCachingAdapter initialized with host: {}, port: {}", redisHost, redisPort);
    }

    @Override
    public void saveModelFactors(ModelFactors modelFactors) {
        if (modelFactors.hasGenericFactorRepresentations()) {
            log.info("Saving generic factor models to Redis...");
            saveGenericUserFactors(modelFactors.getUserFactors());
            saveGenericItemFactors(modelFactors.getItemFactors());
        } else if (modelFactors.hasDatasetRepresentations()) {
            log.info("Saving Dataset factor representations to Redis...");
            saveDatasetFactors(modelFactors.getUserFactorsDataset(), "userFactors", "userId");
            saveDatasetFactors(modelFactors.getItemFactorsDataset(), "itemFactors", "itemId");
        } else {
            log.warn("ModelFactors has neither generic nor Dataset representations. Nothing to save.");
        }
    }

    /**
     * Static nested class to process each partition and save factors to Redis.
     * This class is static to prevent capturing the outer RedisFactorCachingAdapter instance.
     * It must be Serializable itself, and all its fields must be serializable.
     */
    private static class RedisPartitionProcessor implements ForeachPartitionFunction<Row>, Serializable {
        private static final long serialVersionUID = 1L;
        private static final Logger taskLogger = LoggerFactory.getLogger(RedisPartitionProcessor.class);

        private final String processorRedisHost;
        private final int processorRedisPort;
        private final String processorRedisKeyPrefix;
        private final String processorIdColumnName;
        private final String processorFeaturesColumnName;

        public RedisPartitionProcessor(String redisHost, int redisPort, /* String redisPassword, */
                                       String redisKeyPrefix, String idColumnName, String featuresColumnName) {
            this.processorRedisHost = redisHost;
            this.processorRedisPort = redisPort;
            this.processorRedisKeyPrefix = redisKeyPrefix;
            this.processorIdColumnName = idColumnName;
            this.processorFeaturesColumnName = featuresColumnName;
        }

        @Override
        public void call(java.util.Iterator<Row> partitionIterator)  {
            Jedis jedis = null;
            Pipeline pipeline;
            int batchSize = 1000;
            int countInBatch = 0;
            ObjectMapper objectMapper = new ObjectMapper();

            try {
                taskLogger.info("Connecting to Redis at {}:{} for partition processing (key prefix: {})",
                        processorRedisHost, processorRedisPort, processorRedisKeyPrefix);
                jedis = new Jedis(processorRedisHost, processorRedisPort);
                jedis.ping();
                taskLogger.info("Successfully connected to Redis for partition (key prefix: {})", processorRedisKeyPrefix);
                pipeline = jedis.pipelined();

                while (partitionIterator.hasNext()) {
                    Row row = partitionIterator.next();
                    try {
                        int id = row.getInt(row.fieldIndex(processorIdColumnName));
                        Object featuresObject = row.get(row.fieldIndex(processorFeaturesColumnName));
                        String featuresJson;

                        if (featuresObject instanceof List) {
                            featuresJson = objectMapper.writeValueAsString(featuresObject);
                        } else if (featuresObject instanceof Vector) {
                            featuresJson = objectMapper.writeValueAsString(((Vector) featuresObject).toArray());
                        } else if (featuresObject instanceof Seq) {
                            List<Object> javaList = new ArrayList<>();
                            Iterator<?> scalaIterator = ((Seq<?>) featuresObject).iterator();
                            while (scalaIterator.hasNext()) {
                                javaList.add(scalaIterator.next());
                            }
                            featuresJson = objectMapper.writeValueAsString(javaList);
                        } else if (featuresObject == null) {
                            taskLogger.warn("Features object is null for ID {}:{}. Skipping.", processorRedisKeyPrefix, id);
                            continue;
                        } else {
                            taskLogger.warn("Unsupported feature type for ID {}:{}. Type: {}. Skipping.",
                                    processorRedisKeyPrefix, id, featuresObject.getClass().getName());
                            continue;
                        }

                        String redisKey = processorRedisKeyPrefix + ":" + id;
                        pipeline.set(redisKey, featuresJson);

                        countInBatch++;
                        if (countInBatch >= batchSize) {
                            taskLogger.debug("Syncing Redis pipeline for {} (batch size: {})", processorRedisKeyPrefix, countInBatch);
                            pipeline.sync();
                            countInBatch = 0;
                        }
                    } catch (Exception e) {
                        taskLogger.error("Error processing row for Redis persistence (key prefix: {}): Row data: {}. Error: {}",
                                processorRedisKeyPrefix, row.toString(), e.getMessage(), e);
                    }
                }

                if (countInBatch > 0) {
                    taskLogger.info("Syncing final Redis pipeline for {} (batch size: {})", processorRedisKeyPrefix, countInBatch);
                    pipeline.sync();
                }
                taskLogger.info("Finished processing partition for Redis (key prefix: {})", processorRedisKeyPrefix);

            } catch (JedisException e) {
                taskLogger.error("JedisException during Redis operation for partition (key prefix: {}): {}",
                        processorRedisKeyPrefix, e.getMessage(), e);
                throw e;
            } finally {
                if (jedis != null) {
                    try {
                        taskLogger.info("Closing Redis connection for partition (key prefix: {})", processorRedisKeyPrefix);
                        jedis.close();
                    } catch (Exception e) {
                        taskLogger.warn("Error closing Redis connection (key prefix: {}): {}", processorRedisKeyPrefix, e.getMessage(), e);
                    }
                }
            }
        }
    }

    /**
     * Saves factors from a Dataset to Redis.
     *
     * @param factorsDataset The Dataset containing factors
     * @param redisKeyPrefix The prefix for Redis keys
     * @param idColumnName The name of the ID column in the Dataset
     */
    private void saveDatasetFactors(Dataset<Row> factorsDataset, String redisKeyPrefix, String idColumnName) {
        if (factorsDataset == null || factorsDataset.isEmpty()) {
            log.warn("Factors dataset for {} is null or empty. Skipping save.", redisKeyPrefix);
            return;
        }

        log.info("Persisting {} to Redis. Schema:", redisKeyPrefix);
        factorsDataset.printSchema();
        log.info("Sample of {} (first 5 rows, truncate=false):", redisKeyPrefix);
        factorsDataset.show(5, false);

        RedisPartitionProcessor processor = new RedisPartitionProcessor(
                redisHost, redisPort, /* password, */ redisKeyPrefix, idColumnName, "features"
        );

        factorsDataset.foreachPartition(processor);
        log.info("Finished foreachPartition call for {}", redisKeyPrefix);
    }

    /**
     * Saves generic user factors to Redis.
     *
     * @param userFactors List of UserFactor objects
     */
    private void saveGenericUserFactors(List<UserFactor<float[]>> userFactors) {
        if (userFactors == null || userFactors.isEmpty()) {
            log.warn("User factors list is null or empty. Skipping save.");
            return;
        }

        log.info("Saving {} user factors to Redis...", userFactors.size());

        try (Jedis jedis = new Jedis(redisHost, redisPort)) {
            Pipeline pipeline = jedis.pipelined();
            ObjectMapper objectMapper = new ObjectMapper();
            int batchSize = 1000;
            int count = 0;

            for (UserFactor<float[]> userFactor : userFactors) {
                try {
                    int userId = userFactor.getId().getUserId();
                    float[] features = userFactor.getFeatures();

                    // Convert features to JSON
                    String featuresJson = objectMapper.writeValueAsString(features);

                    // Save to Redis
                    String redisKey = "userFactors:" + userId;
                    pipeline.set(redisKey, featuresJson);

                    count++;
                    if (count % batchSize == 0) {
                        pipeline.sync();
                        log.debug("Synced Redis pipeline after {} user factors", count);
                    }
                } catch (Exception e) {
                    log.error("Error saving user factor: {}", e.getMessage(), e);
                }
            }

            // Final sync
            pipeline.sync();
            log.info("Successfully saved {} user factors to Redis", count);
        } catch (Exception e) {
            log.error("Error connecting to Redis: {}", e.getMessage(), e);
        }
    }

    /**
     * Saves generic item factors to Redis.
     *
     * @param itemFactors List of ItemFactor objects
     */
    private void saveGenericItemFactors(List<ItemFactor<float[]>> itemFactors) {
        if (itemFactors == null || itemFactors.isEmpty()) {
            log.warn("Item factors list is null or empty. Skipping save.");
            return;
        }

        log.info("Saving {} item factors to Redis...", itemFactors.size());

        try (Jedis jedis = new Jedis(redisHost, redisPort)) {
            Pipeline pipeline = jedis.pipelined();
            ObjectMapper objectMapper = new ObjectMapper();
            int batchSize = 1000;
            int count = 0;

            for (ItemFactor<float[]> itemFactor : itemFactors) {
                try {
                    int itemId = itemFactor.getId().getMovieId();
                    float[] features = itemFactor.getFeatures();

                    // Convert features to JSON
                    String featuresJson = objectMapper.writeValueAsString(features);

                    // Save to Redis
                    String redisKey = "itemFactors:" + itemId;
                    pipeline.set(redisKey, featuresJson);

                    count++;
                    if (count % batchSize == 0) {
                        pipeline.sync();
                        log.debug("Synced Redis pipeline after {} item factors", count);
                    }
                } catch (Exception e) {
                    log.error("Error saving item factor: {}", e.getMessage(), e);
                }
            }

            // Final sync
            pipeline.sync();
            log.info("Successfully saved {} item factors to Redis", count);
        } catch (Exception e) {
            log.error("Error connecting to Redis: {}", e.getMessage(), e);
        }
    }
}