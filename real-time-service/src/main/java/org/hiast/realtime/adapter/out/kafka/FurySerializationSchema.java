package org.hiast.realtime.adapter.out.kafka;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.fury.Fury;
import org.hiast.model.InteractionEvent;
import org.hiast.realtime.util.FurySerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A Flink SerializationSchema that uses Apache Fury to serialize
 * InteractionEvent objects into byte arrays for Kafka.
 */
public class FurySerializationSchema implements SerializationSchema<InteractionEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(FurySerializationSchema.class);
    private transient Fury fury;

    @Override
    public void open(InitializationContext context) {
        fury = FurySerializationUtils.createConfiguredFury();
        LOG.info("FurySerializationSchema initialized");
    }

    @Override
    public byte[] serialize(InteractionEvent event) {
        if (event == null) {
            LOG.warn("Null event received for serialization");
            return new byte[0];
        }

        try {
            return fury.serialize(event);
        } catch (Exception e) {
            LOG.error("Failed to serialize event with Fury: {}", event, e);
            return new byte[0];
        }
    }
}