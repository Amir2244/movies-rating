package org.hiast.recommendationsapi.adapter.in.web.request;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;

public class InteractionEventRequest {
    @JsonProperty("userId")
    private int userId;

    @JsonProperty("movieId")
    private Integer movieId; // Optional

    @JsonProperty("eventType")
    private String eventType;

    @JsonProperty("timestamp")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "UTC")
    private Instant timestamp;

    @JsonProperty("rating")
    private Double rating; // Optional

    @JsonProperty("eventContext")
    private String eventContext; // Optional

    public InteractionEventRequest() {}

    public int getUserId() { return userId; }
    public void setUserId(int userId) { this.userId = userId; }

    public Integer getMovieId() { return movieId; }
    public void setMovieId(Integer movieId) { this.movieId = movieId; }

    public String getEventType() { return eventType; }
    public void setEventType(String eventType) { this.eventType = eventType; }

    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }

    public Double getRating() { return rating; }
    public void setRating(Double rating) { this.rating = rating; }

    public String getEventContext() { return eventContext; }
    public void setEventContext(String eventContext) { this.eventContext = eventContext; }
} 