package org.hiast.batch.util;

import com.esotericsoftware.kryo.Kryo;
import org.apache.spark.serializer.KryoRegistrator;
import org.hiast.batch.domain.model.ModelFactors;
import org.hiast.batch.domain.model.ProcessedRating;
import org.hiast.ids.MovieId;
import org.hiast.ids.UserId;
import org.hiast.model.RatingValue;
import org.hiast.model.factors.ItemFactor;
import org.hiast.model.factors.UserFactor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Custom CustomKryoRegistrator for registering classes with Kryo serializer.
 * This improves serialization performance for Spark operations.
 */
public class CustomKryoRegistrator implements KryoRegistrator {
    private static final Logger log = LoggerFactory.getLogger(CustomKryoRegistrator.class);

    @Override
    public void registerClasses(Kryo kryo) {
        log.info("Registering classes with Kryo serializer");
        
        // Register domain model classes
        kryo.register(ProcessedRating.class);
        kryo.register(ModelFactors.class);
        
        // Register shared kernel classes
        kryo.register(UserId.class);
        kryo.register(MovieId.class);
        kryo.register(RatingValue.class);
        kryo.register(UserFactor.class);
        kryo.register(ItemFactor.class);
        kryo.register(java.util.HashMap.class);
        kryo.register(java.util.ArrayList.class);
        kryo.register(org.hiast.batch.domain.model.MovieMetaData.class);
        kryo.register(org.hiast.batch.domain.model.DataAnalytics.class);
        // Register Java classes used in the application
        kryo.register(ArrayList.class);
        kryo.register(HashMap.class);
        kryo.register(List.class);
        kryo.register(Map.class);
        kryo.register(Instant.class);
        
        // Register array types
        kryo.register(float[].class);
        kryo.register(double[].class);
        kryo.register(int[].class);
        kryo.register(String[].class);
        kryo.register(Object[].class);
        
        log.info("Kryo registration completed");
    }
}
