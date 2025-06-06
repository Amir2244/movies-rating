package org.hiast.recommendationsapi.adapter.in.web.response;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Response DTO for user recommendations API.
 * This represents the JSON structure returned to API clients.
 */
public class UserRecommendationsResponse {
    
    @JsonProperty("userId")
    private int userId;
    
    @JsonProperty("recommendations")
    private List<MovieRecommendationResponse> recommendations;
    
    @JsonProperty("generatedAt")
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'", timezone = "UTC")
    private Instant generatedAt;
    
    @JsonProperty("modelVersion")
    private String modelVersion;
    
    @JsonProperty("totalRecommendations")
    private int totalRecommendations;
    
    /**
     * Default constructor for JSON deserialization.
     */
    public UserRecommendationsResponse() {
    }
    
    /**
     * Constructor for creating response instances.
     */
    public UserRecommendationsResponse(int userId, 
                                     List<MovieRecommendationResponse> recommendations,
                                     Instant generatedAt, 
                                     String modelVersion) {
        this.userId = userId;
        this.recommendations = recommendations;
        this.generatedAt = generatedAt;
        this.modelVersion = modelVersion;
        this.totalRecommendations = recommendations != null ? recommendations.size() : 0;
    }
    
    public int getUserId() {
        return userId;
    }
    
    public void setUserId(int userId) {
        this.userId = userId;
    }
    
    public List<MovieRecommendationResponse> getRecommendations() {
        return recommendations;
    }
    
    public void setRecommendations(List<MovieRecommendationResponse> recommendations) {
        this.recommendations = recommendations;
        this.totalRecommendations = recommendations != null ? recommendations.size() : 0;
    }
    
    public Instant getGeneratedAt() {
        return generatedAt;
    }
    
    public void setGeneratedAt(Instant generatedAt) {
        this.generatedAt = generatedAt;
    }
    
    public String getModelVersion() {
        return modelVersion;
    }
    
    public void setModelVersion(String modelVersion) {
        this.modelVersion = modelVersion;
    }
    
    public int getTotalRecommendations() {
        return totalRecommendations;
    }
    
    public void setTotalRecommendations(int totalRecommendations) {
        this.totalRecommendations = totalRecommendations;
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserRecommendationsResponse that = (UserRecommendationsResponse) o;
        return userId == that.userId &&
                totalRecommendations == that.totalRecommendations &&
                Objects.equals(recommendations, that.recommendations) &&
                Objects.equals(generatedAt, that.generatedAt) &&
                Objects.equals(modelVersion, that.modelVersion);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(userId, recommendations, generatedAt, modelVersion, totalRecommendations);
    }
    
    @Override
    public String toString() {
        return "UserRecommendationsResponse{" +
                "userId=" + userId +
                ", recommendations=" + recommendations +
                ", generatedAt=" + generatedAt +
                ", modelVersion='" + modelVersion + '\'' +
                ", totalRecommendations=" + totalRecommendations +
                '}';
    }
}
