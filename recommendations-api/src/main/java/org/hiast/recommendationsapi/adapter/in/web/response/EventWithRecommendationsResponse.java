package org.hiast.recommendationsapi.adapter.in.web.response;

import org.hiast.model.InteractionEvent;
import org.hiast.model.MovieRecommendation;

import java.util.List;

/**
 * Response DTO that contains both an interaction event and a list of movie recommendations.
 * This is used to return both the processed event and the recommendations generated for the user.
 */
public class EventWithRecommendationsResponse {
    private final InteractionEvent event;
    private final List<MovieRecommendation> recommendations;

    public EventWithRecommendationsResponse(InteractionEvent event, List<MovieRecommendation> recommendations) {
        this.event = event;
        this.recommendations = recommendations;
    }

    public InteractionEvent getEvent() {
        return event;
    }

    public List<MovieRecommendation> getRecommendations() {
        return recommendations;
    }
}