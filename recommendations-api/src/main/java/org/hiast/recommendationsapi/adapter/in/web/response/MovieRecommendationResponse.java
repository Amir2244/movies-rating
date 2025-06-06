package org.hiast.recommendationsapi.adapter.in.web.response;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Objects;

/**
 * Response DTO for movie recommendation API.
 * This represents the JSON structure for individual movie recommendations.
 */
public class MovieRecommendationResponse {
    
    @JsonProperty("movieId")
    private int movieId;
    
    @JsonProperty("predictedRating")
    private float predictedRating;
    
    @JsonProperty("generatedAt")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "UTC")
    private Instant generatedAt;
    
    /**
     * Default constructor for JSON deserialization.
     */
    public MovieRecommendationResponse() {
    }
    
    /**
     * Constructor for creating response instances.
     */
    public MovieRecommendationResponse(int movieId, float predictedRating, Instant generatedAt) {
        this.movieId = movieId;
        this.predictedRating = predictedRating;
        this.generatedAt = generatedAt;
    }
    
    public int getMovieId() {
        return movieId;
    }
    
    public void setMovieId(int movieId) {
        this.movieId = movieId;
    }
    
    public float getPredictedRating() {
        return predictedRating;
    }
    
    public void setPredictedRating(float predictedRating) {
        this.predictedRating = predictedRating;
    }
    
    public Instant getGeneratedAt() {
        return generatedAt;
    }
    
    public void setGeneratedAt(Instant generatedAt) {
        this.generatedAt = generatedAt;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MovieRecommendationResponse that = (MovieRecommendationResponse) o;
        return movieId == that.movieId &&
                Float.compare(that.predictedRating, predictedRating) == 0 &&
                Objects.equals(generatedAt, that.generatedAt);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(movieId, predictedRating, generatedAt);
    }
    
    @Override
    public String toString() {
        return "MovieRecommendationResponse{" +
                "movieId=" + movieId +
                ", predictedRating=" + predictedRating +
                ", generatedAt=" + generatedAt +
                '}';
    }
}
