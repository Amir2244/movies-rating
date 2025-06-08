package org.hiast.recommendationsapi.adapter.in.web.response;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.List;
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
    
    @JsonProperty("title")
    private String title;
    
    @JsonProperty("genres")
    private List<String> genres;
    
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
    
    /**
     * Constructor for creating response instances with movie metadata.
     */
    public MovieRecommendationResponse(int movieId, float predictedRating, Instant generatedAt, 
                                     String title, List<String> genres) {
        this.movieId = movieId;
        this.predictedRating = predictedRating;
        this.generatedAt = generatedAt;
        this.title = title;
        this.genres = genres;
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
    
    public String getTitle() {
        return title;
    }
    
    public void setTitle(String title) {
        this.title = title;
    }
    
    public List<String> getGenres() {
        return genres;
    }
    
    public void setGenres(List<String> genres) {
        this.genres = genres;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        MovieRecommendationResponse that = (MovieRecommendationResponse) o;
        return movieId == that.movieId &&
                Float.compare(that.predictedRating, predictedRating) == 0 &&
                Objects.equals(generatedAt, that.generatedAt) &&
                Objects.equals(title, that.title) &&
                Objects.equals(genres, that.genres);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(movieId, predictedRating, generatedAt, title, genres);
    }
    
    @Override
    public String toString() {
        return "MovieRecommendationResponse{" +
                "movieId=" + movieId +
                ", predictedRating=" + predictedRating +
                ", generatedAt=" + generatedAt +
                ", title='" + title + '\'' +
                ", genres=" + genres +
                '}';
    }
}
