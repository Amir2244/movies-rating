
package org.hiast.realtime.adapter.in.kafka;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
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

import java.io.IOException;
import java.util.List;

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
