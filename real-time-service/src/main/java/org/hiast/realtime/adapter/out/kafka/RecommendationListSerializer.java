package org.hiast.realtime.adapter.out.kafka;

import org.apache.fury.Fury;
import org.apache.fury.config.Language;
import org.apache.kafka.common.serialization.Serializer;
import org.hiast.events.EventType;
import org.hiast.ids.MovieId;
import org.hiast.ids.UserId;
import org.hiast.model.InteractionEventDetails;
import org.hiast.model.MovieRecommendation;
import org.hiast.model.RatingValue;
import org.hiast.realtime.domain.model.InteractionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;

/**
 * Kafka serializer for lists of MovieRecommendation objects.
 * This serializer uses Apache Fury to convert a list of MovieRecommendation objects to bytes.
 */
public class RecommendationListSerializer implements Serializer<List<MovieRecommendation>> {

    private static final Logger LOG = LoggerFactory.getLogger(RecommendationListSerializer.class);
    private Fury fury;

    @Override
    public void configure(Map<String, ?> configs, boolean isKey) {
        // Initialize Fury with the same configuration as in FuryDeserializationSchema
        fury = Fury.builder()
                .withLanguage(Language.JAVA)
                .requireClassRegistration(true)
                .withAsyncCompilation(true)
                .withRefTracking(true)
                .build();
        fury.register(MovieRecommendation.class);
        fury.register(InteractionEvent.class);
        fury.register(MovieId.class);
        fury.register(Integer.class);
        fury.register(UserId.class);
        fury.register(InteractionEventDetails.class);
        fury.register(Float.class);
        fury.register(RatingValue.class);
        fury.register(Long.class);
        fury.register(EventType.class);
        fury.register(List.class);
    }

    @Override
    public byte[] serialize(String topic, List<MovieRecommendation> data) {
        if (data == null) {
            LOG.warn("Null data received for serialization");
            return null;
        }

        try {
            return fury.serialize(data);
        } catch (Exception e) {
            LOG.error("Error serializing MovieRecommendation list", e);
            throw new RuntimeException("Error serializing MovieRecommendation list", e);
        }
    }

    @Override
    public void close() {
        // No resources to close
    }
}
