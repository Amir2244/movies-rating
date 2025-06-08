package org.hiast.recommendationsapi.adapter.out.persistence.mongodb.document;

import org.springframework.data.annotation.Id;
import org.springframework.data.mongodb.core.mapping.Document;
import org.springframework.data.mongodb.core.mapping.Field;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * MongoDB document representation of user recommendations.
 * This maps to the document structure saved by the batch-training-service.
 */
@Document(collection = "#{@mongoConfig.recommendationsCollection}")
public class UserRecommendationsDocument {
    
    @Id
    private String id;
    
    @Field("userId")
    private int userId;
    
    @Field("recommendations")
    private List<MovieRecommendationDocument> recommendations;
    
    @Field("generatedAt")
    private Instant generatedAt;
    
    @Field("modelVersion")
    private String modelVersion;
    
    /**
     * Default constructor for MongoDB.
     */
    public UserRecommendationsDocument() {
    }
    
    /**
     * Constructor for creating document instances.
     */
    public UserRecommendationsDocument(int userId, 
                                     List<MovieRecommendationDocument> recommendations,
                                     Instant generatedAt, 
                                     String modelVersion) {
        this.userId = userId;
        this.recommendations = recommendations;
        this.generatedAt = generatedAt;
        this.modelVersion = modelVersion;
    }
    
    public String getId() {
        return id;
    }
    
    public void setId(String id) {
        this.id = id;
    }
    
    public int getUserId() {
        return userId;
    }
    
    public void setUserId(int userId) {
        this.userId = userId;
    }
    
    public List<MovieRecommendationDocument> getRecommendations() {
        return recommendations;
    }
    
    public void setRecommendations(List<MovieRecommendationDocument> recommendations) {
        this.recommendations = recommendations;
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
    
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserRecommendationsDocument that = (UserRecommendationsDocument) o;
        return userId == that.userId &&
                Objects.equals(id, that.id) &&
                Objects.equals(recommendations, that.recommendations) &&
                Objects.equals(generatedAt, that.generatedAt) &&
                Objects.equals(modelVersion, that.modelVersion);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(id, userId, recommendations, generatedAt, modelVersion);
    }
    
    @Override
    public String toString() {
        return "UserRecommendationsDocument{" +
                "id='" + id + '\'' +
                ", userId=" + userId +
                ", recommendations=" + recommendations +
                ", generatedAt=" + generatedAt +
                ", modelVersion='" + modelVersion + '\'' +
                '}';
    }
}
