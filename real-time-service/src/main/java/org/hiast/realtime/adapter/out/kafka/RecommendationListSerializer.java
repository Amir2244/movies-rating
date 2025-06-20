package org.hiast.realtime.adapter.out.kafka;

import org.apache.fury.Fury;
import org.apache.kafka.common.serialization.Serializer;
import org.hiast.model.MovieRecommendation;
import org.hiast.realtime.util.FurySerializationUtils;
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
        fury = FurySerializationUtils.createConfiguredFury();
        LOG.info("RecommendationListSerializer initialized");
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
            LOG.error("Error serializing MovieRecommendation list for topic {}: {}", topic, data, e);
            throw new RuntimeException("Error serializing MovieRecommendation list for topic: " + topic, e);
        }
    }

    @Override
    public void close() {
        // No resources to close
    }
}
