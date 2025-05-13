package org.hiast.batch.domain.model;

import org.hiast.ids.MovieId;
import org.hiast.ids.UserId;
import org.hiast.model.RatingValue;
import org.junit.Test;

import java.time.Instant;

import static org.junit.Assert.*;

public class ProcessedRatingTest {

    @Test
    public void testConstructorAndGetters() {
        // Arrange
        UserId userId = UserId.of(123);
        MovieId movieId = MovieId.of(456);
        RatingValue ratingValue = RatingValue.of(4.5);
        Instant timestamp = Instant.ofEpochSecond(1620000000);

        // Act
        ProcessedRating rating = new ProcessedRating(userId, movieId, ratingValue, timestamp);

        // Assert
        assertEquals(123, rating.getUserId());
        assertEquals(456, rating.getMovieId());
        assertEquals(4.5, rating.getRatingActual(), 0.001);
        assertEquals(1620000000, rating.getTimestampEpochSeconds());
        assertEquals(ratingValue.getRatingActual(), rating.getRatingValue().getRatingActual(), 0.001);
        assertEquals(timestamp, rating.getTimestamp());
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorWithNullUserId() {
        // Arrange
        MovieId movieId = MovieId.of(456);
        RatingValue ratingValue = RatingValue.of(4.5);
        Instant timestamp = Instant.ofEpochSecond(1620000000);

        // Act & Assert
        new ProcessedRating(null, movieId, ratingValue, timestamp);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorWithNullMovieId() {
        // Arrange
        UserId userId = UserId.of(123);
        RatingValue ratingValue = RatingValue.of(4.5);
        Instant timestamp = Instant.ofEpochSecond(1620000000);

        // Act & Assert
        new ProcessedRating(userId, null, ratingValue, timestamp);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorWithNullRatingValue() {
        // Arrange
        UserId userId = UserId.of(123);
        MovieId movieId = MovieId.of(456);
        Instant timestamp = Instant.ofEpochSecond(1620000000);

        // Act & Assert
        new ProcessedRating(userId, movieId, null, timestamp);
    }

    @Test(expected = NullPointerException.class)
    public void testConstructorWithNullTimestamp() {
        // Arrange
        UserId userId = UserId.of(123);
        MovieId movieId = MovieId.of(456);
        RatingValue ratingValue = RatingValue.of(4.5);

        // Act & Assert
        new ProcessedRating(userId, movieId, ratingValue, null);
    }

    @Test
    public void testEqualsAndHashCode() {
        // Arrange
        ProcessedRating rating1 = new ProcessedRating(
                UserId.of(123),
                MovieId.of(456),
                RatingValue.of(4.5),
                Instant.ofEpochSecond(1620000000)
        );
        
        ProcessedRating rating2 = new ProcessedRating(
                UserId.of(123),
                MovieId.of(456),
                RatingValue.of(4.5),
                Instant.ofEpochSecond(1620000000)
        );
        
        ProcessedRating differentRating = new ProcessedRating(
                UserId.of(789),
                MovieId.of(456),
                RatingValue.of(4.5),
                Instant.ofEpochSecond(1620000000)
        );

        // Assert
        assertEquals(rating1, rating2);
        assertEquals(rating1.hashCode(), rating2.hashCode());
        assertNotEquals(rating1, differentRating);
        assertNotEquals(rating1.hashCode(), differentRating.hashCode());
    }

    @Test
    public void testToString() {
        // Arrange
        ProcessedRating rating = new ProcessedRating(
                UserId.of(123),
                MovieId.of(456),
                RatingValue.of(4.5),
                Instant.ofEpochSecond(1620000000)
        );

        // Act
        String result = rating.toString();

        // Assert
        assertTrue(result.contains("userId=123"));
        assertTrue(result.contains("movieId=456"));
        assertTrue(result.contains("ratingActual=4.5"));
        assertTrue(result.contains("timestampEpochSeconds=1620000000"));
    }
}