package org.hiast.realtime.util;

import org.apache.fury.Fury;
import org.apache.fury.config.Language;
import org.hiast.events.EventType;
import org.hiast.ids.MovieId;
import org.hiast.ids.UserId;
import org.hiast.model.InteractionEventDetails;
import org.hiast.model.MovieRecommendation;
import org.hiast.model.RatingValue;
import org.hiast.model.InteractionEvent;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Utility class to centralize Fury serialization configuration across the application.
 * Provides methods to create preconfigured Fury instances with all required classes registered.
 */
public final class FurySerializationUtils {

    private FurySerializationUtils() {
        // Utility class - prevent instantiation
    }

    /**
     * Creates and configures a Fury instance with all domain classes registered.
     * This ensures consistent serialization/deserialization behavior across the application.
     *
     * @return A fully configured Fury instance ready for use
     */
    public static Fury createConfiguredFury() {
        Fury fury = Fury.builder()
                .withLanguage(Language.JAVA)
                .requireClassRegistration(true)
                .withAsyncCompilation(true)
                .withRefTracking(false)
                .build();

        // Register all domain classes
        registerDomainClasses(fury);

        return fury;
    }

    /**
     * Registers all domain classes with the provided Fury instance.
     * This method centralizes the class registration logic to ensure consistency.
     *
     * @param fury The Fury instance to register classes with
     */
    public static void registerDomainClasses(Fury fury) {
        // Domain model classes
        fury.register(InteractionEvent.class);
        fury.register(MovieRecommendation.class);
        fury.register(InteractionEventDetails.class);

        // Value objects
        fury.register(UserId.class);
        fury.register(MovieId.class);
        fury.register(RatingValue.class);
        fury.register(EventType.class);

        // Primitive wrappers
        fury.register(Long.class);
        fury.register(Float.class);
        fury.register(Integer.class);
        fury.register(java.time.Instant.class);

        // Collection implementations
        fury.register(ArrayList.class);
        fury.register(Arrays.asList().getClass());
        fury.register(List.class);
    }
}
