
package org.hiast.realtime;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.hiast.realtime.adapter.in.flink.RecommendationRichMapFunction;
import org.hiast.realtime.adapter.in.kafka.FuryDeserializationSchema;
import org.hiast.realtime.config.AppConfig;
import org.hiast.realtime.domain.model.InteractionEvent;

import java.util.Properties;

/**
 * The main class for the Flink streaming job.
 */
public class RealTimeRecommendationsJob {

    public static void main(String[] args) throws Exception {
        // Determine which configuration to load based on program arguments
        final String configFileName = (args.length > 0 && "local".equalsIgnoreCase(args[0]))
                ? "real-time-config-local.properties"
                : "real-time-config.properties";

        // 1. Set up the Flink execution environment
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // 2. Load configuration on the client side just for Kafka properties
        // The RichMapFunction will load its own config on the workers.
        AppConfig appConfig = new AppConfig(configFileName);

        // 3. Configure Kafka consumer
        Properties kafkaProps = new Properties();
        kafkaProps.setProperty("bootstrap.servers", appConfig.getProperty("kafka.bootstrap.servers"));
        kafkaProps.setProperty("group.id", appConfig.getProperty("kafka.group.id"));

        String inputTopic = appConfig.getProperty("kafka.topic.input");

        FlinkKafkaConsumer<InteractionEvent> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic,
                new FuryDeserializationSchema(),
                kafkaProps
        );

        // 4. Create the data stream from Kafka
        DataStream<InteractionEvent> interactionEvents = env.addSource(kafkaConsumer);

        // 5. Process the stream with the RichMapFunction.
        // The function will manage its own dependencies and connections.
        interactionEvents
                .filter(event -> event != null) // Filter out any messages that failed deserialization
                .map(new RecommendationRichMapFunction()) // This is now self-contained
                .name("RealTimeRecommendationProcessing");

        // 6. Execute the Flink job
        env.execute("Real-Time Movie Recommendations Job");
    }
}
