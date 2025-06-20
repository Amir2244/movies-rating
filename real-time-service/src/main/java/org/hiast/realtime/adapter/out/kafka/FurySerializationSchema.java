package org.hiast.realtime.adapter.out.kafka;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.fury.Fury;
import org.apache.fury.config.Language;
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

/**
 * A Flink SerializationSchema that uses Apache Fury to serialize
 * InteractionEvent objects into byte arrays for Kafka.
 */
public class FurySerializationSchema implements SerializationSchema<InteractionEvent> {

    private static final Logger LOG = LoggerFactory.getLogger(FurySerializationSchema.class);
    private transient Fury fury;

    @Override
    public void open(InitializationContext context) {
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
        LOG.info("FurySerializationSchema initialized");
    }

    @Override
    public byte[] serialize(InteractionEvent event) {
        if (event == null) {
            LOG.warn("Null event received for serialization");
            return new byte[0];
        }

        try {
            // Ensure Fury is initialized
            if (fury == null) {
                open(null);
            }
            
            return fury.serialize(event);
        } catch (Exception e) {
            LOG.error("Failed to serialize event with Fury", e);
            return new byte[0];
        }
    }
}