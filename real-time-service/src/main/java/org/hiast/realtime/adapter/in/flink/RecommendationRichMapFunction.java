
package org.hiast.realtime.adapter.in.flink;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.configuration.Configuration;
import org.hiast.realtime.application.port.in.ProcessInteractionEventUseCase;
import org.hiast.realtime.application.service.RealTimeRecommendationService;
import org.hiast.realtime.domain.model.InteractionEvent;
import org.hiast.realtime.adapter.out.notifier.LoggingNotifierAdapter;
import org.hiast.realtime.adapter.out.redis.RedisUserFactorAdapter;
import org.hiast.realtime.adapter.out.redis.RedisVectorSearchAdapter;
import org.hiast.realtime.config.AppConfig;
import redis.clients.jedis.UnifiedJedis;

/**
 * Flink RichMapFunction that acts as a bridge between the Flink DataStream
 * and our application's use case.
 * It manages the lifecycle of dependencies, initializing them in the open() method on each worker.
 */
public class RecommendationRichMapFunction extends RichMapFunction<InteractionEvent, Void> {

    // These transient fields are not serialized. They will be initialized in the open() method
    // on each TaskManager when the job starts.
    private transient ProcessInteractionEventUseCase processInteractionEventUseCase;
    private transient UnifiedJedis jedis;

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
        LoggingNotifierAdapter notifierAdapter = new LoggingNotifierAdapter();

        this.processInteractionEventUseCase = new RealTimeRecommendationService(
                userFactorAdapter,
                vectorSearchAdapter,
                notifierAdapter
        );
    }

    @Override
    public void close() throws Exception {
        // Close the connection when the Flink job is cancelled or finishes.
        if (jedis != null) {
            jedis.close();
        }
        super.close();
    }

    @Override
    public Void map(InteractionEvent event) throws Exception {
        // The use case is now initialized and ready to be used.
        processInteractionEventUseCase.processEvent(event);
        return null;
    }
}