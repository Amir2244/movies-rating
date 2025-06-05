package org.hiast.recommendationsapi.domain.model;

import org.hiast.ids.UserId;

import java.io.Serializable;
import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * Domain model representing user recommendations in the API context.
 * This is a domain-specific representation that uses shared-kernel value objects.
 */
public final class UserRecommendations implements Serializable {
    private static final long serialVersionUID = 1L;

    private final UserId userId;
    private final List<MovieRecommendation> recommendations;
    private final Instant generatedAt;
    private final String modelVersion;

    /**
     * Constructor for UserRecommendations.
     *
     * @param userId          The user ID value object.
     * @param recommendations The list of movie recommendations.
     * @param generatedAt     The timestamp when recommendations were generated.
     * @param modelVersion    The version of the model that generated recommendations.
     */
    public UserRecommendations(UserId userId, 
                              List<MovieRecommendation> recommendations,
                              Instant generatedAt, 
                              String modelVersion) {
        this.userId = Objects.requireNonNull(userId, "userId cannot be null");
        this.recommendations = Objects.requireNonNull(recommendations, "recommendations cannot be null");
        this.generatedAt = Objects.requireNonNull(generatedAt, "generatedAt cannot be null");
        this.modelVersion = Objects.requireNonNull(modelVersion, "modelVersion cannot be null");
    }

    public UserId getUserId() {
        return userId;
    }

    public List<MovieRecommendation> getRecommendations() {
        return recommendations;
    }

    public Instant getGeneratedAt() {
        return generatedAt;
    }

    public String getModelVersion() {
        return modelVersion;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        UserRecommendations that = (UserRecommendations) o;
        return Objects.equals(userId, that.userId) &&
                Objects.equals(recommendations, that.recommendations) &&
                Objects.equals(generatedAt, that.generatedAt) &&
                Objects.equals(modelVersion, that.modelVersion);
    }

    @Override
    public int hashCode() {
        return Objects.hash(userId, recommendations, generatedAt, modelVersion);
    }

    @Override
    public String toString() {
        return "UserRecommendations{" +
                "userId=" + userId +
                ", recommendations=" + recommendations +
                ", generatedAt=" + generatedAt +
                ", modelVersion='" + modelVersion + '\'' +
                '}';
    }
}
