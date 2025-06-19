package org.hiast.realtime.adapter.out.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.hiast.ids.UserId;
import org.hiast.model.MovieRecommendation;
import org.hiast.realtime.application.port.out.RecommendationNotifierPort;
import org.hiast.realtime.config.AppConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Properties;

/**
 * An adapter that sends recommendations to Kafka.
 * This implementation sends the recommendations to a Kafka topic.
 */
public class KafkaNotifierAdapter implements RecommendationNotifierPort {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaNotifierAdapter.class);
    private final Producer<String, List<MovieRecommendation>> producer;
    private final String outputTopic;

    /**
     * Creates a new KafkaNotifierAdapter with the specified configuration.
     * @param appConfig the application configuration
     */
    public KafkaNotifierAdapter(AppConfig appConfig) {
        Properties props = new Properties();
        props.put("bootstrap.servers", appConfig.getProperty("kafka.bootstrap.servers"));
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.hiast.realtime.adapter.out.kafka.RecommendationListSerializer");

        this.producer = new KafkaProducer<>(props);

        // Get the output topic from config or use default if not found
        String configTopic = appConfig.getProperty("kafka.topic.output");
        this.outputTopic = (configTopic != null) ? configTopic : "recommendations";

        LOG.info("KafkaNotifierAdapter initialized with output topic: {}", outputTopic);
    }

    @Override
    public void notify(UserId userId, List<MovieRecommendation> recommendations) {
        if (userId == null || recommendations == null || recommendations.isEmpty()) {
            LOG.warn("Cannot send null or empty recommendations to Kafka");
            return;
        }

        try {
            // Convert the user ID to a string
            String key = String.valueOf(userId.getUserId());
            ProducerRecord<String, List<MovieRecommendation>> record = 
                new ProducerRecord<>(outputTopic, key, recommendations);

            producer.send(record, (metadata, exception) -> {
                if (exception != null) {
                    LOG.error("Error sending recommendations to Kafka", exception);
                } else {
                    LOG.info("Recommendations sent to Kafka topic {} for user {}: partition={}, offset={}",
                            outputTopic, userId.getUserId(), metadata.partition(), metadata.offset());
                }
            });
        } catch (Exception e) {
            LOG.error("Failed to send recommendations to Kafka", e);
        }
    }

    /**
     * Closes the Kafka producer.
     * This method should be called when the adapter is no longer needed.
     */
    public void close() {
        if (producer != null) {
            producer.close();
            LOG.info("Kafka producer closed");
        }
    }
}
