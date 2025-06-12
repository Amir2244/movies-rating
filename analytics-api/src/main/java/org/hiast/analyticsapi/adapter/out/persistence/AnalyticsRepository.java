package org.hiast.analyticsapi.adapter.out.persistence;

import org.springframework.data.domain.Page;
import org.springframework.data.domain.Pageable;
import org.springframework.data.mongodb.repository.MongoRepository;
import org.springframework.data.mongodb.repository.Query;
import org.springframework.stereotype.Repository;

import java.util.Date;
import java.util.List;
import java.util.Optional;

/**
 * Spring Data MongoDB repository for analytics data.
 * Provides basic CRUD operations and custom queries.
 */
@Repository
public interface AnalyticsRepository extends MongoRepository<AnalyticsDocument, String> {

    /**
     * Finds analytics by analyticsId field.
     */
    Optional<AnalyticsDocument> findByAnalyticsId(String analyticsId);

    /**
     * Finds analytics by type.
     */
    List<AnalyticsDocument> findByType(String type);

    /**
     * Finds analytics by type with pagination.
     */
    Page<AnalyticsDocument> findByType(String type, Pageable pageable);

    /**
     * Finds analytics by multiple types.
     */
    List<AnalyticsDocument> findByTypeIn(List<String> types);

    /**
     * Finds analytics by multiple types with pagination.
     */
    Page<AnalyticsDocument> findByTypeIn(List<String> types, Pageable pageable);

    /**
     * Finds analytics generated between two dates.
     */
    List<AnalyticsDocument> findByGeneratedAtBetween(Date fromDate, Date toDate);

    /**
     * Finds analytics generated between two dates with pagination.
     */
    Page<AnalyticsDocument> findByGeneratedAtBetween(Date fromDate, Date toDate, Pageable pageable);

    /**
     * Finds analytics generated after a specific date.
     */
    List<AnalyticsDocument> findByGeneratedAtAfter(Date fromDate);

    /**
     * Finds analytics generated before a specific date.
     */
    List<AnalyticsDocument> findByGeneratedAtBefore(Date toDate);

    /**
     * Finds analytics by type and date range.
     */
    Page<AnalyticsDocument> findByTypeInAndGeneratedAtBetween(
            List<String> types, Date fromDate, Date toDate, Pageable pageable);

    /**
     * Finds analytics by description containing a keyword.
     */
    @Query("{'description': {$regex: ?0, $options: 'i'}}")
    List<AnalyticsDocument> findByDescriptionContainingIgnoreCase(String keyword);

    /**
     * Finds analytics by description containing a keyword with pagination.
     */
    @Query("{'description': {$regex: ?0, $options: 'i'}}")
    Page<AnalyticsDocument> findByDescriptionContainingIgnoreCase(String keyword, Pageable pageable);

    /**
     * Counts analytics by type.
     */
    long countByType(String type);

    /**
     * Counts analytics generated between dates.
     */
    long countByGeneratedAtBetween(Date fromDate, Date toDate);

    /**
     * Counts analytics by type and date range.
     */
    long countByTypeInAndGeneratedAtBetween(List<String> types, Date fromDate, Date toDate);

    /**
     * Gets the latest analytics by generatedAt.
     */
    Optional<AnalyticsDocument> findTopByOrderByGeneratedAtDesc();

    /**
     * Gets the oldest analytics by generatedAt.
     */
    Optional<AnalyticsDocument> findTopByOrderByGeneratedAtAsc();

    /**
     * Gets count of distinct types.
     */
    @Query(value = "{}", count = true)
    long countDistinctType();

    /**
     * Checks if analytics exist for a specific type.
     */
    boolean existsByType(String type);
} 