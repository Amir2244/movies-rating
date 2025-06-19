
package org.hiast.realtime.application.service;

import org.hiast.realtime.application.port.in.ProcessInteractionEventUseCase;
import org.hiast.realtime.application.port.out.RecommendationNotifierPort;
import org.hiast.realtime.application.port.out.UserFactorPort;
import org.hiast.realtime.application.port.out.VectorSearchPort;
import org.hiast.realtime.domain.model.InteractionEvent;
import org.hiast.model.factors.UserFactor;
import org.hiast.model.MovieRecommendation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;

/**
 * The core application service that implements the use case.
 * It orchestrates the flow: get user factor -> find similar items -> notify.
 */
public class RealTimeRecommendationService implements ProcessInteractionEventUseCase {

    private static final Logger LOG = LoggerFactory.getLogger(RealTimeRecommendationService.class);
    private static final int TOP_N_RECOMMENDATIONS = 5;

    private final UserFactorPort userFactorPort;
    private final VectorSearchPort vectorSearchPort;
    private final RecommendationNotifierPort recommendationNotifierPort;

    public RealTimeRecommendationService(UserFactorPort userFactorPort, VectorSearchPort vectorSearchPort, RecommendationNotifierPort recommendationNotifierPort) {
        this.userFactorPort = userFactorPort;
        this.vectorSearchPort = vectorSearchPort;
        this.recommendationNotifierPort = recommendationNotifierPort;
    }

    @Override
    public void processEvent(InteractionEvent event) {
        if (event == null || event.getUserId() == null || event.getDetails() == null) {
            LOG.warn("Received a null or invalid interaction event.");
            return;
        }

        // Get the event type and its weight
        double eventWeight = event.getDetails().getEventType().getWeight();
        LOG.info("Processing event for user: {} with event type: {} and weight: {}", 
                event.getUserId().getUserId(), 
                event.getDetails().getEventType(),
                eventWeight);

        // Skip processing for events with zero weight
        if (eventWeight <= 0) {
            LOG.info("Skipping event with zero or negative weight: {}", event.getDetails().getEventType());
            return;
        }

        Optional<UserFactor<float[]>> userFactorOpt = userFactorPort.findUserFactorById(event.getUserId());

        if (userFactorOpt.isEmpty()) {
            LOG.warn("Could not find user factor for user: {}", event.getUserId().getUserId());
            return;
        }

        UserFactor<float[]> userFactor = userFactorOpt.get();

        // Use the event weight to determine the number of recommendations
        // More important events get more recommendations
        int numRecommendations = Math.max(1, (int) Math.ceil(eventWeight * TOP_N_RECOMMENDATIONS / 5.0));
        LOG.info("Generating {} recommendations based on event weight {}", numRecommendations, eventWeight);

        // Pass the event weight to the vector search port to influence the rating calculation
        List<MovieRecommendation> recommendations = vectorSearchPort.findSimilarItems(userFactor, numRecommendations, eventWeight);
        LOG.info("Generated {} recommendations with event weight {} applied to ratings", recommendations.size(), eventWeight);

        if (recommendations.isEmpty()) {
            LOG.info("No recommendations found for user: {}", event.getUserId().getUserId());
        } else {
            // Include the original event in the notification
            recommendationNotifierPort.notify(event.getUserId(), recommendations);
        }

        // Mark the event as processed
        event.setProcessed(true);
    }
}
