
package org.hiast.realtime;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.hiast.realtime.adapter.in.flink.RecommendationMapFunction;
import org.hiast.realtime.adapter.in.kafka.FuryDeserializationSchema;
import org.hiast.realtime.adapter.out.notifier.LoggingNotifierAdapter;
import org.hiast.realtime.adapter.out.redis.RedisUserFactorAdapter;
import org.hiast.realtime.adapter.out.redis.RedisVectorSearchAdapter;
import org.hiast.realtime.application.service.RealTimeRecommendationService;
import org.hiast.realtime.config.AppConfig;
import org.hiast.realtime.domain.model.InteractionEvent;
import redis.clients.jedis.JedisPool;

import java.util.Objects;
import java.util.Properties;

/**
 * The main class for the Flink streaming job.
 */
public class RealTimeRecommendationsJob {

    public static void main(String[] args) throws Exception {
        // 1. Set up the Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. Load configuration
        AppConfig appConfig = new AppConfig("real-time-config.properties");

        // 3. Configure Kafka consumer
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", appConfig.getProperty("kafka.bootstrap.servers"));
        kafkaProps.setProperty("group.id", appConfig.getProperty("kafka.group.id"));

        String inputTopic = appConfig.getProperty("kafka.topic.input");

        KafkaSource<InteractionEvent> kafkaSource = KafkaSource.<InteractionEvent>builder()
                .setBootstrapServers(kafkaProps.getProperty("bootstrap.servers"))
                .setTopics(inputTopic)
                .setGroupId(kafkaProps.getProperty("group.id"))
                .setValueOnlyDeserializer(new FuryDeserializationSchema())
                .build();


// 4. Create the data stream from Kafka
        DataStream<InteractionEvent> interactionEvents = env.fromSource(kafkaSource, WatermarkStrategy.noWatermarks(), "Kafka Source");
        // 5. Set up dependencies for the hexagonal architecture
        JedisPool jedisPool = appConfig.getJedisPool();

        RedisUserFactorAdapter userFactorAdapter = new RedisUserFactorAdapter(jedisPool);
        RedisVectorSearchAdapter vectorSearchAdapter = new RedisVectorSearchAdapter(jedisPool);
        LoggingNotifierAdapter notifierAdapter = new LoggingNotifierAdapter();

        RealTimeRecommendationService recommendationService = new RealTimeRecommendationService(
                userFactorAdapter,
                vectorSearchAdapter,
                notifierAdapter
        );

        // 6. Process the stream
        interactionEvents
                .filter(Objects::nonNull) // Filter out any messages that failed deserialization
                .map(new RecommendationMapFunction(recommendationService))
                .name("RealTimeRecommendationProcessing");

        // 7. Execute the Flink job
        env.execute("Real-Time Movie Recommendations Job");
    }
}
