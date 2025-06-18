
package org.hiast.realtime.adapter.out.notifier;

import org.hiast.ids.UserId;
import org.hiast.model.MovieRecommendation;
import org.hiast.realtime.application.port.out.RecommendationNotifierPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;

/**
 * A simple notifier that logs the recommendations.
 * This can be replaced with a Kafka producer, WebSocket sender, etc.
 */
public class LoggingNotifierAdapter implements RecommendationNotifierPort {

    private static final Logger LOG = LoggerFactory.getLogger(LoggingNotifierAdapter.class);

    @Override
    public void notify(UserId userId, List<MovieRecommendation> recommendations) {
        LOG.info("Recommendations for user {}: {}", userId.getUserId(), recommendations);
    }
}