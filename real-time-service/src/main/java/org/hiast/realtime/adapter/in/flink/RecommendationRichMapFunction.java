
package org.hiast.realtime.adapter.in.flink;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.hiast.realtime.application.port.in.ProcessInteractionEventUseCase;
import org.hiast.realtime.application.service.RealTimeRecommendationService;
import org.hiast.model.InteractionEvent;
import org.hiast.realtime.adapter.out.kafka.KafkaNotifierAdapter;
import org.hiast.realtime.adapter.out.redis.RedisUserFactorAdapter;
import org.hiast.realtime.adapter.out.redis.RedisVectorSearchAdapter;
import org.hiast.realtime.config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import redis.clients.jedis.UnifiedJedis;

/**
 * Flink RichMapFunction that acts as a bridge between the Flink DataStream
 * and our application's use case.
 * It manages the lifecycle of dependencies, initializing them in the open() method on each worker.
 * It returns the processed event so it can be sent back to Kafka.
 */
public class RecommendationRichMapFunction extends RichMapFunction<InteractionEvent, InteractionEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(RecommendationRichMapFunction.class);


    private transient ProcessInteractionEventUseCase processInteractionEventUseCase;
    private transient UnifiedJedis jedis;
    private transient KafkaNotifierAdapter kafkaNotifierAdapter;

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        // Each parallel instance of this function will open its own connection.
        // The config file is loaded from the resources of the JAR on the TaskManager.
        AppConfig appConfig = new AppConfig("real-time-config.properties");
        this.jedis = appConfig.getUnifiedJedis();

        // Initialize all dependencies here, on the worker node.
        RedisUserFactorAdapter userFactorAdapter = new RedisUserFactorAdapter(this.jedis);
        RedisVectorSearchAdapter vectorSearchAdapter = new RedisVectorSearchAdapter(this.jedis);

        this.kafkaNotifierAdapter = new KafkaNotifierAdapter(appConfig);

        this.processInteractionEventUseCase = new RealTimeRecommendationService(
                userFactorAdapter,
                vectorSearchAdapter,
                kafkaNotifierAdapter
        );

        LOG.info("RecommendationRichMapFunction initialized with KafkaNotifierAdapter");
    }

    @Override
    public void close() throws Exception {
        if (jedis != null) {
            jedis.close();
        }
        if (kafkaNotifierAdapter != null) {
            kafkaNotifierAdapter.close();
        }
        super.close();
    }

    @Override
    public InteractionEvent map(InteractionEvent event) throws Exception {
        if (event == null) {
            LOG.warn("Received null event");
            processInteractionEventUseCase.processEvent(null);
            return null;
        }

        LOG.info("Processing event for user: {}", event.getUserId().getUserId());
        processInteractionEventUseCase.processEvent(event);

        return event;
    }
}
