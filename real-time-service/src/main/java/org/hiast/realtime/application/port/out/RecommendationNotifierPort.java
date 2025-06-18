package org.hiast.realtime.application.port.out;

import org.hiast.ids.UserId;
import org.hiast.model.MovieRecommendation;
import java.util.List;

/**
 * Output Port for sending notifications or results.
 * This could be an adapter for Kafka, WebSockets, or just logging.
 */
public interface RecommendationNotifierPort {
    void notify(UserId userId, List<MovieRecommendation> recommendations);
}
