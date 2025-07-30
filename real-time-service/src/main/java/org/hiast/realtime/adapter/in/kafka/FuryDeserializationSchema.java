
package org.hiast.realtime.adapter.in.kafka;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.fury.Fury;
import org.hiast.model.InteractionEvent;
import org.hiast.realtime.util.FurySerializationUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

/**
 * A Flink DeserializationSchema that uses Apache Fury to deserialize
 * byte arrays from Kafka into InteractionEvent objects.
 */
public class FuryDeserializationSchema implements DeserializationSchema<InteractionEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(FuryDeserializationSchema.class);
    private transient Fury fury;

    @Override
    public void open(InitializationContext context) {
        fury = FurySerializationUtils.createConfiguredFury();
        LOG.info("FuryDeserializationSchema initialized");
    }

    @Override
    public InteractionEvent deserialize(byte[] message) throws IOException {
        if (message == null || message.length == 0) {
            return null;
        }
        try {
            return (InteractionEvent) fury.deserialize(message);
        } catch (Exception e) {
            LOG.error("Failed to deserialize message with Fury", e);
            LOG.info("Reinitializing Fury instance after deserialization failure");
            fury = FurySerializationUtils.createConfiguredFury();

            try {
                return (InteractionEvent) fury.deserialize(message);
            } catch (Exception retryException) {
                LOG.error("Failed to deserialize message with Fury after reinitialization", retryException);
                return null;
            }
        }
    }

    @Override
    public boolean isEndOfStream(InteractionEvent nextElement) {
        return false;
    }

    @Override
    public TypeInformation<InteractionEvent> getProducedType() {
        return TypeInformation.of(InteractionEvent.class);
    }
}
