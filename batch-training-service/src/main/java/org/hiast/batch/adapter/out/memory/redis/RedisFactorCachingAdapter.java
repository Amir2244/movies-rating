package org.hiast.batch.adapter.out.memory.redis;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.apache.spark.ml.linalg.Vector;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.hiast.batch.adapter.out.memory.redis.util.VectorMetadata;
import org.hiast.batch.adapter.out.memory.redis.util.VectorSerializationException;
import org.hiast.batch.adapter.out.memory.redis.util.VectorSerializationUtil;
import org.hiast.batch.application.port.out.FactorCachingPort;
import org.hiast.batch.domain.model.ModelFactors;
import org.hiast.ids.MovieId;
import org.hiast.ids.UserId;
import org.hiast.model.factors.ItemFactor;
import org.hiast.model.factors.UserFactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisException;

import java.io.Serializable;
import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import scala.collection.Seq;

/**
 * Redis adapter for caching user and item factors as vectors in Redis.
 * This implementation stores factor vectors as byte arrays, enabling Redis to function
 * as a vector database for similarity search and recommendation queries.
 *
 * Key features:
 * - Stores vectors as binary data using Redis HSET operations
 * - Maintains vector metadata for Redis Stack/RedisSearch compatibility
 * - Supports both Spark Dataset and generic factor representations
 * - Provides efficient batch operations with pipelining
 * - Follows hexagonal architecture patterns
 */
public class RedisFactorCachingAdapter implements FactorCachingPort, AutoCloseable {
    private static final Logger log = LoggerFactory.getLogger(RedisFactorCachingAdapter.class);

    // Batch processing constants
    private static final int BATCH_SIZE = 5000;
    private static final int VECTOR_BATCH_SIZE = 1000; // Smaller batches for vector operations

    // Redis connection pool constants
    private static final int MAX_TOTAL = 100;
    private static final int MAX_IDLE = 20;
    private static final int MIN_IDLE = 5;
    private static final int MAX_WAIT_MILLIS = 30000;
    private static final boolean TEST_ON_BORROW = true;
    private static final boolean TEST_ON_RETURN = true;
    private static final int CONNECTION_TIMEOUT = 2000;
    private static final int SO_TIMEOUT = 2000;

    // Vector storage constants
    private static final String MODEL_VERSION = "als-v1.0";
    private static final String VECTOR_INDEX_PREFIX = "vector_idx";

    private final String redisHost;
    private final int redisPort;
    private final JedisPool jedisPool;
    private final ObjectMapper objectMapper; // Kept for backward compatibility

    public RedisFactorCachingAdapter(String redisHost, int redisPort) {
        this.redisHost = redisHost;
        this.redisPort = redisPort;
        this.jedisPool = createJedisPool();
        this.objectMapper = new ObjectMapper();
        log.info("RedisFactorCachingAdapter initialized with host: {}, port: {} (Vector Database Mode)",
                redisHost, redisPort);

        // Test Redis connection and log vector storage capabilities
        testRedisConnection();
    }

    private JedisPool createJedisPool() {
        JedisPoolConfig poolConfig = new JedisPoolConfig();
        poolConfig.setMaxTotal(MAX_TOTAL);
        poolConfig.setMaxIdle(MAX_IDLE);
        poolConfig.setMinIdle(MIN_IDLE);
        poolConfig.setMaxWaitMillis(MAX_WAIT_MILLIS);
        poolConfig.setTestOnBorrow(TEST_ON_BORROW);
        poolConfig.setTestOnReturn(TEST_ON_RETURN);
        poolConfig.setTestWhileIdle(true);
        poolConfig.setTimeBetweenEvictionRunsMillis(30000);
        poolConfig.setNumTestsPerEvictionRun(3);
        poolConfig.setMinEvictableIdleTimeMillis(1800000);

        return new JedisPool(poolConfig, redisHost, redisPort, CONNECTION_TIMEOUT);
    }

    /**
     * Tests Redis connection and logs vector storage capabilities.
     */
    private void testRedisConnection() {
        try (Jedis jedis = jedisPool.getResource()) {
            String pong = jedis.ping();
            log.info("Redis connection test successful: {}", pong);

            // Log Redis version and capabilities
            String info = jedis.info("server");
            if (info.contains("redis_version")) {
                String[] lines = info.split("\r\n");
                for (String line : lines) {
                    if (line.startsWith("redis_version:")) {
                        log.info("Redis server version: {}", line.substring(14));
                        break;
                    }
                }
            }

            log.info("Vector database mode enabled - factors will be stored as byte arrays");
        } catch (Exception e) {
            log.warn("Redis connection test failed: {}", e.getMessage());
        }
    }

    @Override
    public void saveModelFactors(ModelFactors modelFactors) {
        log.info("Saving model factors to Redis vector database...");

        if (modelFactors.hasGenericFactorRepresentations()) {
            log.info("Saving generic factor models as vectors to Redis...");
            saveGenericUserFactorsAsVectors(modelFactors.getUserFactors());
            saveGenericItemFactorsAsVectors(modelFactors.getItemFactors());
        } else if (modelFactors.hasDatasetRepresentations()) {
            log.info("Saving Dataset factor representations as vectors to Redis...");
            saveDatasetFactorsAsVectors(modelFactors.getUserFactorsDataset(), "user", "userId");
            saveDatasetFactorsAsVectors(modelFactors.getItemFactorsDataset(), "item", "itemId");
        } else {
            log.warn("ModelFactors has neither generic nor Dataset representations. Nothing to save.");
        }

        log.info("Model factors saved to Redis vector database successfully");
    }

    /**
     * Static nested class to process each partition and save factors as vectors to Redis.
     * This class is static to prevent capturing the outer RedisFactorCachingAdapter instance.
     * It must be Serializable itself, and all its fields must be serializable.
     *
     * Enhanced to support vector database operations with byte array storage.
     */
    private static class RedisVectorPartitionProcessor implements ForeachPartitionFunction<Row>, Serializable {
        private static final long serialVersionUID = 2L; // Updated version
        private static final Logger taskLogger = LoggerFactory.getLogger(RedisVectorPartitionProcessor.class);

        private final String processorRedisHost;
        private final int processorRedisPort;
        private final String processorEntityType; // "user" or "item"
        private final String processorIdColumnName;
        private final String processorFeaturesColumnName;

        public RedisVectorPartitionProcessor(String redisHost, int redisPort,
                                           String entityType, String idColumnName, String featuresColumnName) {
            this.processorRedisHost = redisHost;
            this.processorRedisPort = redisPort;
            this.processorEntityType = entityType;
            this.processorIdColumnName = idColumnName;
            this.processorFeaturesColumnName = featuresColumnName;
        }

        @Override
        public void call(java.util.Iterator<Row> partitionIterator) {
            Jedis jedis = null;
            Pipeline pipeline = null;
            int countInBatch = 0;

            try {
                taskLogger.info("Connecting to Redis at {}:{} for vector partition processing (entity type: {})",
                        processorRedisHost, processorRedisPort, processorEntityType);
                jedis = new Jedis(processorRedisHost, processorRedisPort);
                jedis.ping();
                taskLogger.info("Successfully connected to Redis for vector partition (entity type: {})", processorEntityType);
                pipeline = jedis.pipelined();

                while (partitionIterator.hasNext()) {
                    Row row = partitionIterator.next();
                    try {
                        int id = row.getInt(row.fieldIndex(processorIdColumnName));
                        Object featuresObject = row.get(row.fieldIndex(processorFeaturesColumnName));
                        float[] vectorArray = null;

                        // Convert various feature formats to float array
                        if (featuresObject instanceof List) {
                            @SuppressWarnings("unchecked")
                            List<Number> featureList = (List<Number>) featuresObject;
                            vectorArray = new float[featureList.size()];
                            for (int i = 0; i < featureList.size(); i++) {
                                vectorArray[i] = featureList.get(i).floatValue();
                            }
                        } else if (featuresObject instanceof Vector) {
                            double[] doubleArray = ((Vector) featuresObject).toArray();
                            vectorArray = new float[doubleArray.length];
                            for (int i = 0; i < doubleArray.length; i++) {
                                vectorArray[i] = (float) doubleArray[i];
                            }
                        } else if (featuresObject instanceof Seq) {
                            @SuppressWarnings("unchecked")
                            scala.collection.Seq<Object> seq = (scala.collection.Seq<Object>) featuresObject;
                            scala.collection.Iterator<Object> scalaIterator = seq.iterator();
                            List<Float> tempList = new ArrayList<>();
                            while (scalaIterator.hasNext()) {
                                Object item = scalaIterator.next();
                                if (item instanceof Number) {
                                    tempList.add(((Number) item).floatValue());
                                }
                            }
                            vectorArray = new float[tempList.size()];
                            for (int i = 0; i < tempList.size(); i++) {
                                vectorArray[i] = tempList.get(i);
                            }
                        } else if (featuresObject == null) {
                            taskLogger.warn("Features object is null for ID {}:{}. Skipping.", processorEntityType, id);
                            continue;
                        } else {
                            taskLogger.warn("Unsupported feature type for ID {}:{}. Type: {}. Skipping.",
                                    processorEntityType, id, featuresObject.getClass().getName());
                            continue;
                        }

                        // Serialize vector to byte array and store with metadata
                        if (vectorArray != null && vectorArray.length > 0) {
                            storeVectorInRedis(pipeline, id, vectorArray, processorEntityType);
                        } else {
                            taskLogger.warn("Empty or invalid vector for ID {}:{}. Skipping.", processorEntityType, id);
                            continue;
                        }

                        countInBatch++;
                        if (countInBatch >= VECTOR_BATCH_SIZE) {
                            taskLogger.debug("Syncing Redis pipeline for {} vectors (batch size: {})", processorEntityType, countInBatch);
                            pipeline.sync();
                            countInBatch = 0;
                        }
                    } catch (Exception e) {
                        taskLogger.error("Error processing row for Redis vector persistence (entity type: {}): Row data: {}. Error: {}",
                                processorEntityType, row.toString(), e.getMessage(), e);
                    }
                }

                if (countInBatch > 0) {
                    taskLogger.info("Syncing final Redis pipeline for {} vectors (batch size: {})", processorEntityType, countInBatch);
                    pipeline.sync();
                }
                taskLogger.info("Finished processing vector partition for Redis (entity type: {})", processorEntityType);

            } catch (JedisException e) {
                taskLogger.error("JedisException during Redis vector operation for partition (entity type: {}): {}",
                        processorEntityType, e.getMessage(), e);
                throw e;
            } finally {
                if (jedis != null) {
                    try {
                        taskLogger.info("Closing Redis connection for vector partition (entity type: {})", processorEntityType);
                        jedis.close();
                    } catch (Exception e) {
                        taskLogger.warn("Error closing Redis connection (entity type: {}): {}", processorEntityType, e.getMessage(), e);
                    }
                }
            }
        }

        /**
         * Stores a vector in Redis using HSET with binary data and metadata.
         */
        private static void storeVectorInRedis(Pipeline pipeline, int entityId, float[] vector, String entityType) {
            try {
                // Validate and serialize vector
                VectorSerializationUtil.validateVector(vector);
                byte[] vectorBytes = VectorSerializationUtil.serializeVector(vector);

                // Create Redis key
                String redisKey = entityType.equals(VectorSerializationUtil.USER_ENTITY_TYPE)
                    ? VectorSerializationUtil.createUserFactorKey(entityId)
                    : VectorSerializationUtil.createItemFactorKey(entityId);

                // Create metadata
                VectorMetadata metadata = entityType.equals(VectorSerializationUtil.USER_ENTITY_TYPE)
                    ? VectorMetadata.forUser(entityId, vector.length, MODEL_VERSION)
                    : VectorMetadata.forItem(entityId, vector.length, MODEL_VERSION);

                // Store vector and metadata using HSET
                Map<String, String> fields = new HashMap<>();
                fields.put(VectorSerializationUtil.ENTITY_ID_FIELD, String.valueOf(entityId));
                fields.put(VectorSerializationUtil.ENTITY_TYPE_FIELD, entityType);
                fields.put(VectorSerializationUtil.DIMENSION_FIELD, String.valueOf(vector.length));
                fields.put(VectorSerializationUtil.TIMESTAMP_FIELD, metadata.getTimestamp().toString());

                // Store metadata fields
                pipeline.hset(redisKey, fields);

                // Store vector as binary field
                pipeline.hset(redisKey.getBytes(), VectorSerializationUtil.VECTOR_FIELD.getBytes(), vectorBytes);

            } catch (VectorSerializationException e) {
                throw new RuntimeException("Failed to store vector for entity " + entityId + " of type " + entityType, e);
            }
        }
    }

    /**
     * Saves factors from a Dataset to Redis as vectors.
     *
     * @param factorsDataset The Dataset containing factors
     * @param entityType The entity type ("user" or "item")
     * @param idColumnName The name of the ID column in the Dataset
     */
    private void saveDatasetFactorsAsVectors(Dataset<Row> factorsDataset, String entityType, String idColumnName) {
        if (factorsDataset == null || factorsDataset.isEmpty()) {
            log.warn("Factors dataset for {} is null or empty. Skipping vector save.", entityType);
            return;
        }

        log.info("Persisting {} factors as vectors to Redis. Schema:", entityType);
        factorsDataset.printSchema();
        log.info("Sample of {} factors (first 5 rows, truncate=false):", entityType);
        factorsDataset.show(5, false);

        RedisVectorPartitionProcessor processor = new RedisVectorPartitionProcessor(
                redisHost, redisPort, entityType, idColumnName, "features"
        );

        factorsDataset.foreachPartition(processor);
        log.info("Finished foreachPartition call for {} vectors", entityType);
    }

    /**
     * Saves generic user factors to Redis as vectors.
     *
     * @param userFactors List of UserFactor objects
     */
    private void saveGenericUserFactorsAsVectors(List<UserFactor<float[]>> userFactors) {
        if (userFactors == null || userFactors.isEmpty()) {
            log.warn("User factors list is null or empty. Skipping vector save.");
            return;
        }

        log.info("Saving {} user factors as vectors to Redis...", userFactors.size());

        try (Jedis jedis = jedisPool.getResource()) {
            Pipeline pipeline = jedis.pipelined();
            int count = 0;

            for (UserFactor<float[]> userFactor : userFactors) {
                try {
                    int userId = userFactor.getId().getUserId();
                    float[] features = userFactor.getFeatures();

                    // Store as vector with metadata
                    storeUserVectorInRedis(pipeline, userId, features);

                    count++;
                    if (count % VECTOR_BATCH_SIZE == 0) {
                        pipeline.sync();
                        log.debug("Synced Redis pipeline after {} user vectors", count);
                    }
                } catch (Exception e) {
                    log.error("Error saving user vector for user {}: {}",
                             userFactor.getId().getUserId(), e.getMessage(), e);
                }
            }

            pipeline.sync();
            log.info("Successfully saved {} user factors as vectors to Redis", count);
        } catch (Exception e) {
            log.error("Error connecting to Redis for user vector storage: {}", e.getMessage(), e);
        }
    }

    /**
     * Saves generic item factors to Redis as vectors.
     *
     * @param itemFactors List of ItemFactor objects
     */
    private void saveGenericItemFactorsAsVectors(List<ItemFactor<float[]>> itemFactors) {
        if (itemFactors == null || itemFactors.isEmpty()) {
            log.warn("Item factors list is null or empty. Skipping vector save.");
            return;
        }

        log.info("Saving {} item factors as vectors to Redis...", itemFactors.size());

        try (Jedis jedis = jedisPool.getResource()) {
            Pipeline pipeline = jedis.pipelined();
            int count = 0;

            for (ItemFactor<float[]> itemFactor : itemFactors) {
                try {
                    int itemId = itemFactor.getId().getMovieId();
                    float[] features = itemFactor.getFeatures();

                    // Store as vector with metadata
                    storeItemVectorInRedis(pipeline, itemId, features);

                    count++;
                    if (count % VECTOR_BATCH_SIZE == 0) {
                        pipeline.sync();
                        log.debug("Synced Redis pipeline after {} item vectors", count);
                    }
                } catch (Exception e) {
                    log.error("Error saving item vector for item {}: {}",
                             itemFactor.getId().getMovieId(), e.getMessage(), e);
                }
            }

            pipeline.sync();
            log.info("Successfully saved {} item factors as vectors to Redis", count);
        } catch (Exception e) {
            log.error("Error connecting to Redis for item vector storage: {}", e.getMessage(), e);
        }
    }

    /**
     * Stores a user vector in Redis with metadata.
     */
    private void storeUserVectorInRedis(Pipeline pipeline, int userId, float[] vector) {
        try {
            VectorSerializationUtil.validateVector(vector);
            byte[] vectorBytes = VectorSerializationUtil.serializeVector(vector);
            String redisKey = VectorSerializationUtil.createUserFactorKey(userId);

            VectorMetadata metadata = VectorMetadata.forUser(userId, vector.length, MODEL_VERSION);

            // Store metadata fields
            Map<String, String> fields = new HashMap<>();
            fields.put(VectorSerializationUtil.ENTITY_ID_FIELD, String.valueOf(userId));
            fields.put(VectorSerializationUtil.ENTITY_TYPE_FIELD, VectorSerializationUtil.USER_ENTITY_TYPE);
            fields.put(VectorSerializationUtil.DIMENSION_FIELD, String.valueOf(vector.length));
            fields.put(VectorSerializationUtil.TIMESTAMP_FIELD, metadata.getTimestamp().toString());

            pipeline.hset(redisKey, fields);
            pipeline.hset(redisKey.getBytes(), VectorSerializationUtil.VECTOR_FIELD.getBytes(), vectorBytes);

        } catch (VectorSerializationException e) {
            throw new RuntimeException("Failed to store user vector for user " + userId, e);
        }
    }

    /**
     * Stores an item vector in Redis with metadata.
     */
    private void storeItemVectorInRedis(Pipeline pipeline, int itemId, float[] vector) {
        try {
            VectorSerializationUtil.validateVector(vector);
            byte[] vectorBytes = VectorSerializationUtil.serializeVector(vector);
            String redisKey = VectorSerializationUtil.createItemFactorKey(itemId);

            VectorMetadata metadata = VectorMetadata.forItem(itemId, vector.length, MODEL_VERSION);

            // Store metadata fields
            Map<String, String> fields = new HashMap<>();
            fields.put(VectorSerializationUtil.ENTITY_ID_FIELD, String.valueOf(itemId));
            fields.put(VectorSerializationUtil.ENTITY_TYPE_FIELD, VectorSerializationUtil.ITEM_ENTITY_TYPE);
            fields.put(VectorSerializationUtil.DIMENSION_FIELD, String.valueOf(vector.length));
            fields.put(VectorSerializationUtil.TIMESTAMP_FIELD, metadata.getTimestamp().toString());

            pipeline.hset(redisKey, fields);
            pipeline.hset(redisKey.getBytes(), VectorSerializationUtil.VECTOR_FIELD.getBytes(), vectorBytes);

        } catch (VectorSerializationException e) {
            throw new RuntimeException("Failed to store item vector for item " + itemId, e);
        }
    }

    @Override
    public void close() {
        if (jedisPool != null) {
            try {
                jedisPool.close();
                log.info("Redis connection pool closed");
            } catch (Exception e) {
                log.error("Error closing Redis connection pool: {}", e.getMessage(), e);
            }
        }
    }
}