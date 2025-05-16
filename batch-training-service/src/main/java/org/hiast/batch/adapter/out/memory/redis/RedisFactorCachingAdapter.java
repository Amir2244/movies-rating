package org.hiast.batch.adapter.out.memory.redis;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.api.java.function.ForeachPartitionFunction;
import org.hiast.batch.domain.model.ModelFactors;
import org.hiast.batch.application.port.out.FactorCachingPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import redis.clients.jedis.Jedis;
import redis.clients.jedis.Pipeline;
import redis.clients.jedis.exceptions.JedisException;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.ml.linalg.Vector;
import com.fasterxml.jackson.databind.ObjectMapper;


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
        log.info("Saving user factors to Redis...");
        saveFactors(modelFactors.getUserFactors(), "userFactors",
                "userId", redisHost, redisPort /*, redisPassword */);
        log.info("User factors saved.");
        log.info("Saving item factors to Redis...");
        saveFactors(modelFactors.getItemFactors(), "itemFactors",
                "itemId", redisHost, redisPort /*, redisPassword */);
        log.info("Item factors saved.");
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
     */
    private void saveFactors(Dataset<Row> factorsDataset, String redisKeyPrefix,
                             String idColumnName,
                             String host, int port /*, String password */) {
        if (factorsDataset == null || factorsDataset.isEmpty()) {
            log.warn("Factors dataset for {} is null or empty. Skipping save.", redisKeyPrefix);
            return;
        }

        log.info("Persisting {} to Redis. Schema:", redisKeyPrefix);
        factorsDataset.printSchema();
        log.info("Sample of {} (first 5 rows, truncate=false):", redisKeyPrefix);
        factorsDataset.show(5, false);

        RedisPartitionProcessor processor = new RedisPartitionProcessor(
                host, port, /* password, */ redisKeyPrefix, idColumnName, "features"
        );

        factorsDataset.foreachPartition(processor);
        log.info("Finished foreachPartition call for {}", redisKeyPrefix);
    }
}