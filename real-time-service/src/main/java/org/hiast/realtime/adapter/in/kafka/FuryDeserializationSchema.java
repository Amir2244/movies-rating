
package org.hiast.realtime.adapter.in.kafka;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.fury.Fury;
import org.apache.fury.config.Language;
import org.hiast.realtime.domain.model.InteractionEvent;
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
        // Initialize Fury on each task manager.
        fury = Fury.builder()
                .withLanguage(Language.JAVA)
                .requireClassRegistration(false)
                .withRefTracking(true)
                .build();
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
            return null;
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
