package org.hiast.recommendationsapi.adapter.out.persistence.mongodb.document;

import org.springframework.data.mongodb.core.mapping.Field;

import java.time.Instant;
import java.util.List;
import java.util.Objects;

/**
 * MongoDB embedded document representing a movie recommendation.
 * This maps to the nested document structure within UserRecommendationsDocument.
 */
public class MovieRecommendationDocument {
    
    @Field("movieId")
    private int movieId;
    
    @Field("rating")
    private float rating;
    
    @Field("generatedAt")
    private Instant generatedAt;
    
    @Field("movieTitle")
    private String title;
    
    @Field("movieGenres")
    private List<String> genres;
    
    /**
     * Default constructor for MongoDB.
     */
    public MovieRecommendationDocument() {
    }
    
    /**
     * Constructor for creating document instances.
     */
    public MovieRecommendationDocument(int movieId, float rating, Instant generatedAt) {
        this.movieId = movieId;
        this.rating = rating;
        this.generatedAt = generatedAt;
    }
    
    /**
     * Constructor for creating document instances with movie metadata.
     */
    public MovieRecommendationDocument(int movieId, float rating, Instant generatedAt, 
                                     String title, List<String> genres) {
        this.movieId = movieId;
        this.rating = rating;
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
    
    public float getRating() {
        return rating;
    }
    
    public void setRating(float rating) {
        this.rating = rating;
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
        MovieRecommendationDocument that = (MovieRecommendationDocument) o;
        return movieId == that.movieId &&
                Float.compare(that.rating, rating) == 0 &&
                Objects.equals(generatedAt, that.generatedAt) &&
                Objects.equals(title, that.title) &&
                Objects.equals(genres, that.genres);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(movieId, rating, generatedAt, title, genres);
    }
    
    @Override
    public String toString() {
        return "MovieRecommendationDocument{" +
                "movieId=" + movieId +
                ", rating=" + rating +
                ", generatedAt=" + generatedAt +
                ", title='" + title + '\'' +
                ", genres=" + genres +
                '}';
    }
}
