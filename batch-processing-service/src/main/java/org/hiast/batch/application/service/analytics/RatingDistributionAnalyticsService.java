package org.hiast.batch.application.service.analytics;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.functions;
import org.hiast.batch.domain.exception.AnalyticsCollectionException;

import org.hiast.model.AnalyticsType;
import org.hiast.model.DataAnalytics;
import org.hiast.model.analytics.AnalyticsMetrics;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

/**
 * Service responsible for collecting rating distribution analytics.
 * Analyzes the distribution of ratings across the dataset to understand
 * user rating patterns and preferences.
 *
 * This service provides insights into how users distribute their ratings
 * and identifies rating patterns that can inform recommendation algorithms.
 */
public class RatingDistributionAnalyticsService implements AnalyticsCollector {

    private static final Logger log = LoggerFactory.getLogger(RatingDistributionAnalyticsService.class);

    @Override
    public List<DataAnalytics> collectAnalytics(Dataset<Row> ratingsDf,
                                                Dataset<Row> moviesData,
                                                Dataset<Row> tagsData) {

        if (!canProcess(ratingsDf, moviesData, tagsData)) {
            throw new AnalyticsCollectionException("RATING_DISTRIBUTION", "Insufficient data for rating distribution analytics");
        }

        try {
            log.info("Collecting rating distribution analytics...");

            AnalyticsMetrics metrics = AnalyticsMetrics.builder();

            // Collect rating distribution metrics
            collectRatingDistributionMetrics(ratingsDf, metrics);

            // Collect rating statistics
            collectRatingStatistics(ratingsDf, metrics);

            // Collect rating patterns
            collectRatingPatterns(ratingsDf, metrics);

            log.info("Rating distribution analytics collection completed with {} metrics", metrics.size());

            return Collections.singletonList(new DataAnalytics(
                    "rating_distribution_" + UUID.randomUUID().toString().substring(0, 8),
                    Instant.now(),
                    AnalyticsType.RATING_DISTRIBUTION,
                    metrics.build(),
                    "Rating distribution analytics showing rating patterns and user preferences"
            ));

        } catch (Exception e) {
            log.error("Error collecting rating distribution analytics: {}", e.getMessage(), e);
            throw new AnalyticsCollectionException("RATING_DISTRIBUTION", e.getMessage(), e);
        }
    }

    /**
     * Collects basic rating distribution metrics.
     */
    private void collectRatingDistributionMetrics(Dataset<Row> ratingsDf, AnalyticsMetrics metrics) {
        log.debug("Collecting rating distribution metrics...");

        // Get rating distribution
        Dataset<Row> ratingDistribution = ratingsDf.groupBy("ratingActual")
                .count()
                .orderBy("ratingActual");

        // Add distribution data - exact same format as original
        ratingDistribution.collectAsList().forEach(row -> {
            String ratingKey = "rating_" + row.getDouble(0);
            metrics.addMetric(ratingKey, row.getLong(1));
        });

        log.debug("Rating distribution metrics collected");
    }

    /**
     * Collects rating statistics.
     */
    private void collectRatingStatistics(Dataset<Row> ratingsDf, AnalyticsMetrics metrics) {
        log.debug("Collecting rating statistics...");

        // Calculate statistics - exact same as original
        Row ratingStats = ratingsDf.agg(
                functions.count("ratingActual").as("totalRatings"),
                functions.avg("ratingActual").as("avgRating"),
                functions.min("ratingActual").as("minRating"),
                functions.max("ratingActual").as("maxRating"),
                functions.stddev("ratingActual").as("stdDevRating")
        ).first();

        // Add statistics - exact same keys as original
        metrics.addMetric("totalRatings", ratingStats.getLong(0))
               .addMetric("avgRating", ratingStats.getDouble(1))
               .addMetric("minRating", ratingStats.getDouble(2))
               .addMetric("maxRating", ratingStats.getDouble(3))
               .addMetric("stdDevRating", ratingStats.getDouble(4));

        log.debug("Rating statistics collected");
    }

    /**
     * Collects additional rating patterns and insights.
     */
    private void collectRatingPatterns(Dataset<Row> ratingsDf, AnalyticsMetrics metrics) {
        log.debug("Collecting rating patterns...");

        // Calculate rating distribution percentages
        long totalRatings = ratingsDf.count();

        // Count ratings by category
        long highRatings = ratingsDf.filter(functions.col("ratingActual").geq(4.0)).count();
        long mediumRatings = ratingsDf.filter(functions.col("ratingActual").between(2.5, 3.9)).count();
        long lowRatings = ratingsDf.filter(functions.col("ratingActual").lt(2.5)).count();

        metrics.addMetric("highRatings", highRatings)
               .addMetric("mediumRatings", mediumRatings)
               .addMetric("lowRatings", lowRatings)
               .addMetric("highRatingsPercentage", (double) highRatings / totalRatings * 100)
               .addMetric("mediumRatingsPercentage", (double) mediumRatings / totalRatings * 100)
               .addMetric("lowRatingsPercentage", (double) lowRatings / totalRatings * 100);

        // Calculate rating diversity
        long uniqueRatingValues = ratingsDf.select("ratingActual").distinct().count();
        metrics.addMetric("uniqueRatingValues", uniqueRatingValues);

        // Calculate most and least common ratings
        Dataset<Row> ratingCounts = ratingsDf.groupBy("ratingActual")
                .count()
                .orderBy(functions.desc("count"));

        Row mostCommon = ratingCounts.first();
        Row leastCommon = ratingCounts.orderBy(functions.asc("count")).first();

        metrics.addMetric("mostCommonRating", mostCommon.getDouble(0))
               .addMetric("mostCommonRatingCount", mostCommon.getLong(1))
               .addMetric("leastCommonRating", leastCommon.getDouble(0))
               .addMetric("leastCommonRatingCount", leastCommon.getLong(1));

        // Calculate rating variance and skewness indicators
        Row varianceStats = ratingsDf.agg(
                functions.expr("percentile(ratingActual, 0.25)").as("q1"),
                functions.expr("percentile(ratingActual, 0.5)").as("median"),
                functions.expr("percentile(ratingActual, 0.75)").as("q3")
        ).first();

        metrics.addMetric("ratingQ1", varianceStats.getDouble(0))
               .addMetric("ratingMedian", varianceStats.getDouble(1))
               .addMetric("ratingQ3", varianceStats.getDouble(2))
               .addMetric("ratingIQR", varianceStats.getDouble(2) - varianceStats.getDouble(0));

        log.debug("Rating patterns collected");
    }

    @Override
    public String getAnalyticsType() {
        return "RATING_DISTRIBUTION";
    }

    @Override
    public boolean canProcess(Dataset<Row> ratingsDf, Dataset<Row> moviesData, Dataset<Row> tagsData) {
        return ratingsDf != null && !ratingsDf.isEmpty();
    }

    @Override
    public int getPriority() {
        return 30; // Medium priority for rating distribution
    }
}
