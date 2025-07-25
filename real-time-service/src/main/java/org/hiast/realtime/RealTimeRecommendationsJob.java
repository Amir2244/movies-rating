package org.hiast.realtime;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;
import org.hiast.realtime.adapter.in.flink.RecommendationRichMapFunction;
import org.hiast.realtime.adapter.in.kafka.FuryDeserializationSchema;
import org.hiast.realtime.adapter.out.kafka.FurySerializationSchema;
import org.hiast.realtime.config.AppConfig;
import org.hiast.model.InteractionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Objects;
import java.util.Properties;

/**
 * The main class for the Flink streaming job.
 * It reads events from Kafka, processes them, and sends the processed events back to Kafka.
 */
public class RealTimeRecommendationsJob {

    private static final Logger LOG = LoggerFactory.getLogger(RealTimeRecommendationsJob.class);

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
        // FIX: Read the new 'processed' topic. The 'output' topic ('recommendations')
        // is now used exclusively by the KafkaNotifierAdapter for actual recommendations.
        String processedEventsTopic = appConfig.getProperty("kafka.topic.processed");

        if (processedEventsTopic == null || processedEventsTopic.isEmpty()) {
            processedEventsTopic = "processed-events"; // Default output topic
        }

        LOG.info("Using input topic: {}, processed events topic: {}", inputTopic, processedEventsTopic);

        FlinkKafkaConsumer<InteractionEvent> kafkaConsumer = new FlinkKafkaConsumer<>(
                inputTopic,
                new FuryDeserializationSchema(),
                kafkaProps
        );

        // 4. Create the data stream from Kafka
        DataStream<InteractionEvent> interactionEvents = env.addSource(kafkaConsumer);

        // 5. Process the stream with the RichMapFunction and get the processed events
        // The RichMapFunction internally uses KafkaNotifierAdapter to send recommendations to the 'recommendations' topic.
        DataStream<InteractionEvent> processedEvents = interactionEvents
                .filter(Objects::nonNull)
                .map(new RecommendationRichMapFunction())
                .name("RealTimeRecommendationProcessing");

        // 6. Configure Kafka producer for processed events
        // We'll use the same serialization mechanism (Fury) for consistency
        FlinkKafkaProducer<InteractionEvent> kafkaProducer = new FlinkKafkaProducer<>(
                processedEventsTopic, // FIX: Send to the 'processed_events' topic
                new FurySerializationSchema(),
                kafkaProps
        );

        // 7. Send processed events back to Kafka
        processedEvents
                .filter(event -> event.isProcessed()) // Only send processed events
                .addSink(kafkaProducer)
                .name("ProcessedEventsSink");

        // 8. Execute the Flink job
        env.execute("Real-Time Movie Recommendations Job");
    }
}