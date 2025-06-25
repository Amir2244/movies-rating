package org.hiast.batch.adapter.out.persistence.mongodb;

import com.mongodb.ConnectionString;
import com.mongodb.MongoClientSettings;
import com.mongodb.MongoBulkWriteException;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.ReplaceOptions;
import com.mongodb.client.model.WriteModel;
import com.mongodb.client.model.ReplaceOneModel;
import com.mongodb.client.model.BulkWriteOptions;
import org.bson.Document;
import org.hiast.batch.application.port.out.ResultPersistencePort;
import org.hiast.batch.config.MongoConfig;
import org.hiast.model.MovieRecommendation;
import org.hiast.model.UserRecommendations;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Optimized adapter implementing ResultPersistencePort to save recommendation results to MongoDB.
 * Features rate limiting, exponential backoff, and adaptive batch sizing to prevent MongoDB throttling.
 * Implements Serializable for Spark distributed operations.
 */
public class MongoResultPersistenceAdapter implements ResultPersistencePort {
    private static final long serialVersionUID = 1L;
    private static final Logger log = LoggerFactory.getLogger(MongoResultPersistenceAdapter.class);
    
    // Batch size configuration with adaptive sizing
    private static final int MIN_BATCH_SIZE = 1000;
    private static final int MAX_BATCH_SIZE = 6000;
    private static final int INITIAL_BATCH_SIZE = 5000;
    
    // Connection pool configuration
    private static final int MAX_POOL_SIZE = 50; // Reduced to prevent overwhelming MongoDB
    private static final int MIN_POOL_SIZE = 5;
    private static final int MAX_IDLE_TIME_MS = 60000;
    private static final int CONNECTION_TIMEOUT_MS = 30000;
    private static final int SOCKET_TIMEOUT_MS = 120000;
    
    // Rate limiting configuration
    private static final int MAX_OPERATIONS_PER_SECOND = 5000; // Adjust based on MongoDB capacity
    private static final int MAX_CONCURRENT_WRITES = 10; // Limit concurrent bulk operations
    
    // Retry configuration
    private static final int MAX_RETRY_ATTEMPTS = 3;
    private static final long INITIAL_RETRY_DELAY_MS = 100;
    private static final long MAX_RETRY_DELAY_MS = 5000;
    
    // Circuit breaker configuration
    private static final int FAILURE_THRESHOLD = 5;
    private static final long CIRCUIT_BREAKER_TIMEOUT_MS = 80000;

    private final MongoConfig mongoConfig;
    private  MongoClient mongoClient; // Marked transient as MongoClient is not serializable
    private  Semaphore writeSemaphore;
    private  RateLimiter rateLimiter;
    private  ExecutorService executorService;
    
    // Adaptive batch sizing
    private final AtomicInteger currentBatchSize;
    private final AtomicLong lastSuccessfulWrite;
    
    // Circuit breaker state
    private final AtomicInteger consecutiveFailures;
    private volatile long circuitBreakerOpenTime;
    private volatile boolean circuitBreakerOpen;

    public MongoResultPersistenceAdapter(MongoConfig mongoConfig) {
        this.mongoConfig = mongoConfig;
        this.currentBatchSize = new AtomicInteger(INITIAL_BATCH_SIZE);
        this.lastSuccessfulWrite = new AtomicLong(System.currentTimeMillis());
        this.consecutiveFailures = new AtomicInteger(0);
        this.circuitBreakerOpen = false;
        initializeTransientFields();
        
        log.info("MongoResultPersistenceAdapter initialized with adaptive throttling - Initial batch size: {}, Max ops/sec: {}", 
                INITIAL_BATCH_SIZE, MAX_OPERATIONS_PER_SECOND);
    }
    
    /**
     * Initialize transient fields. Called from constructor and after deserialization.
     */
    private void initializeTransientFields() {
        this.mongoClient = createMongoClient();
        this.writeSemaphore = new Semaphore(MAX_CONCURRENT_WRITES);
        this.rateLimiter = new RateLimiter(MAX_OPERATIONS_PER_SECOND);
        this.executorService = Executors.newFixedThreadPool(MAX_CONCURRENT_WRITES);
    }
    
    /**
     * Ensure the MongoDB client is connected. Called before operations to handle deserialization.
     */
    private synchronized void ensureConnected() {
        if (mongoClient == null) {
            log.info("Reconnecting to MongoDB after deserialization");
            initializeTransientFields();
        }
    }

    private MongoClient createMongoClient() {
        ConnectionString connectionString = new ConnectionString(mongoConfig.getConnectionString());
        MongoClientSettings settings = MongoClientSettings.builder()
                .applyConnectionString(connectionString)
                .applyToConnectionPoolSettings(builder -> builder
                        .maxSize(MAX_POOL_SIZE)
                        .minSize(MIN_POOL_SIZE)
                        .maxConnectionIdleTime(MAX_IDLE_TIME_MS, TimeUnit.MILLISECONDS)
                        .maxConnectionLifeTime(10, TimeUnit.MINUTES))
                .applyToSocketSettings(builder -> builder
                        .connectTimeout(CONNECTION_TIMEOUT_MS, TimeUnit.MILLISECONDS)
                        .readTimeout(SOCKET_TIMEOUT_MS, TimeUnit.MILLISECONDS))
                .applyToServerSettings(builder -> builder
                        .heartbeatFrequency(10000, TimeUnit.MILLISECONDS))
                .retryWrites(true)
                .build();
        return MongoClients.create(settings);
    }

    @Override
    public boolean saveUserRecommendations(List<UserRecommendations> userRecommendationsList) {
        // Ensure connection is established
        ensureConnected();
        
        if (userRecommendationsList == null || userRecommendationsList.isEmpty()) {
            log.warn("No user recommendations to save. Skipping MongoDB persistence.");
            return false;
        }

        // Check circuit breaker
        if (isCircuitBreakerOpen()) {
            log.warn("Circuit breaker is open. Rejecting write operation.");
            return false;
        }

        log.info("Saving {} user recommendations to MongoDB with adaptive batch size: {}", 
                userRecommendationsList.size(), currentBatchSize.get());
        
        try {
            MongoDatabase database = mongoClient.getDatabase(mongoConfig.getDatabase());
            MongoCollection<Document> collection = database.getCollection(mongoConfig.getRecommendationsCollection());

            List<Future<Integer>> futures = new ArrayList<>();
            List<WriteModel<Document>> currentBatch = new ArrayList<>();
            int totalProcessed = 0;

            for (UserRecommendations userRecs : userRecommendationsList) {
                try {
                    Document document = convertToDocument(userRecs);
                    ReplaceOptions options = new ReplaceOptions().upsert(true);
                    currentBatch.add(new ReplaceOneModel<>(
                            Filters.eq("userId", userRecs.getUserId().getUserId()),
                            document,
                            options
                    ));

                    totalProcessed++;
                    
                    // Execute batch when it reaches the current adaptive batch size
                    if (currentBatch.size() >= currentBatchSize.get()) {
                        List<WriteModel<Document>> batchToProcess = new ArrayList<>(currentBatch);
                        currentBatch.clear();
                        
                        Future<Integer> future = executorService.submit(() -> 
                            executeBulkOperationWithThrottling(collection, batchToProcess)
                        );
                        futures.add(future);
                    }
                } catch (Exception e) {
                    log.error("Error preparing recommendations for user {}: {}",
                            userRecs != null ? userRecs.getUserId().getUserId() : "unknown", e.getMessage());
                }
            }

            // Execute remaining operations
            if (!currentBatch.isEmpty()) {
                Future<Integer> future = executorService.submit(() ->
                    executeBulkOperationWithThrottling(collection, currentBatch)
                );
                futures.add(future);
            }

            // Wait for all operations to complete and collect results
            int successCount = 0;
            for (Future<Integer> future : futures) {
                try {
                    successCount += future.get(5, TimeUnit.MINUTES);
                } catch (TimeoutException e) {
                    log.error("Bulk operation timed out");
                    future.cancel(true);
                } catch (Exception e) {
                    log.error("Error waiting for bulk operation: {}", e.getMessage());
                }
            }

            log.info("Successfully saved {}/{} user recommendations to MongoDB. Current batch size: {}",
                    successCount, totalProcessed, currentBatchSize.get());

            // Reset circuit breaker on success
            if (successCount > 0) {
                consecutiveFailures.set(0);
                circuitBreakerOpen = false;
            }

            return successCount > 0;
        } catch (Exception e) {
            log.error("Failed to save user recommendations to MongoDB: {}", e.getMessage(), e);
            handleFailure();
            return false;
        }
    }

    private int executeBulkOperationWithThrottling(MongoCollection<Document> collection, 
                                                   List<WriteModel<Document>> operations) {
        int attempt = 0;
        long delayMs = INITIAL_RETRY_DELAY_MS;
        
        while (attempt < MAX_RETRY_ATTEMPTS) {
            try {
                // Acquire write permit
                writeSemaphore.acquire();
                
                try {
                    // Apply rate limiting
                    rateLimiter.acquire(operations.size());
                    
                    // Execute bulk operation
                    long startTime = System.currentTimeMillis();
                    BulkWriteOptions options = new BulkWriteOptions()
                            .ordered(false)
                            .bypassDocumentValidation(false);
                    
                    // Execute bulk write and get result count
                    int modifiedCount;
                    modifiedCount = collection.bulkWrite(operations, options).getModifiedCount();

                    long duration = System.currentTimeMillis() - startTime;
                    
                    // Adjust batch size based on performance
                    adjustBatchSize(duration, operations.size(), true);
                    
                    // Update last successful write time
                    lastSuccessfulWrite.set(System.currentTimeMillis());
                    
                    log.debug("Bulk write completed in {}ms for {} operations", duration, operations.size());
                    return modifiedCount;
                    
                } finally {
                    writeSemaphore.release();
                }
                
            } catch (MongoBulkWriteException e) {
                log.warn("Bulk write error on attempt {}: {}", attempt + 1, e.getMessage());
                
                // Some writings may have succeeded - try to extract count from exception
                int successCount = 0;
                try {
                    // Different MongoDB driver versions handle this differently
                    // Try to get the writing counts if available
                    successCount = e.getWriteResult().getModifiedCount();
                } catch (Exception ex) {
                    // If we can't get the count, assume partial failure
                    log.debug("Could not extract success count from bulk write exception");
                }
                
                if (successCount > 0) {
                    adjustBatchSize(0, operations.size(), false);
                    return successCount;
                }
                
                // Retry with exponential backoff
                attempt++;
                if (attempt < MAX_RETRY_ATTEMPTS) {
                    try {
                        Thread.sleep(delayMs);
                        delayMs = Math.min(delayMs * 2, MAX_RETRY_DELAY_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return 0;
                    }
                }
                
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                log.error("Thread interrupted while waiting for write permit");
                return 0;
                
            } catch (Exception e) {
                log.error("Error executing bulk operation on attempt {}: {}", attempt + 1, e.getMessage());
                adjustBatchSize(0, operations.size(), false);
                
                attempt++;
                if (attempt < MAX_RETRY_ATTEMPTS) {
                    try {
                        Thread.sleep(delayMs);
                        delayMs = Math.min(delayMs * 2, MAX_RETRY_DELAY_MS);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                        return 0;
                    }
                }
            }
        }
        
        handleFailure();
        return 0;
    }

    /**
     * Adjusts batch size based on operation performance
     */
    private void adjustBatchSize(long durationMs, int operationCount, boolean success) {
        if (success) {
            // If operation was fast, try increasing batch size
            if (durationMs < 1000 && operationCount == currentBatchSize.get()) {
                int newSize = Math.min(currentBatchSize.get() + 100, MAX_BATCH_SIZE);
                currentBatchSize.set(newSize);
                log.debug("Increased batch size to {}", newSize);
            }
        } else {
            // On failure, reduce batch size
            int newSize = Math.max(currentBatchSize.get() / 2, MIN_BATCH_SIZE);
            currentBatchSize.set(newSize);
            log.info("Reduced batch size to {} due to write failure", newSize);
        }
    }

    /**
     * Handle operation failure and circuit breaker logic
     */
    private void handleFailure() {
        int failures = consecutiveFailures.incrementAndGet();
        if (failures >= FAILURE_THRESHOLD) {
            circuitBreakerOpen = true;
            circuitBreakerOpenTime = System.currentTimeMillis();
            log.error("Circuit breaker opened after {} consecutive failures", failures);
        }
    }

    /**
     * Check if circuit breaker is open
     */
    private boolean isCircuitBreakerOpen() {
        if (!circuitBreakerOpen) {
            return false;
        }
        
        // Check if timeout has passed
        if (System.currentTimeMillis() - circuitBreakerOpenTime > CIRCUIT_BREAKER_TIMEOUT_MS) {
            circuitBreakerOpen = false;
            consecutiveFailures.set(0);
            log.info("Circuit breaker closed after timeout");
            return false;
        }
        
        return true;
    }

    /**
     * Converts a UserRecommendations object to a MongoDB Document.
     */
    private Document convertToDocument(UserRecommendations userRecs) {
        if (userRecs == null) {
            throw new IllegalArgumentException("UserRecommendations cannot be null");
        }

        Document document = new Document();
        document.append("userId", userRecs.getUserId().getUserId());
        document.append("generatedAt", Date.from(userRecs.getGeneratedAt()));
        document.append("modelVersion", userRecs.getModelVersion());

        List<Document> recommendationDocs = new ArrayList<>();
        if (userRecs.getRecommendations() != null) {
            for (MovieRecommendation rec : userRecs.getRecommendations()) {
                if (rec != null) {
                    Document recDoc = new Document()
                            .append("movieId", rec.getMovieId().getMovieId())
                            .append("rating", rec.getPredictedRating())
                            .append("generatedAt", Date.from(rec.getGeneratedAt()));

                    // Add movie metadata if available
                    if (rec.getMovieTitle() != null) {
                        recDoc.append("movieTitle", rec.getMovieTitle());
                    }
                    if (rec.getMovieGenres() != null && !rec.getMovieGenres().isEmpty()) {
                        recDoc.append("movieGenres", rec.getMovieGenres());
                    }

                    recommendationDocs.add(recDoc);
                }
            }
        }

        document.append("recommendations", recommendationDocs);
        return document;
    }

    /**
     * Simple rate limiter implementation
     */
    private static class RateLimiter implements Serializable {
        private static final long serialVersionUID = 1L;
        private final int maxOpsPerSecond;
        private transient Semaphore permits;

        public RateLimiter(int maxOpsPerSecond) {
            this.maxOpsPerSecond = maxOpsPerSecond;
            initialize();
        }
        
        private void initialize() {
            this.permits = new Semaphore(maxOpsPerSecond);
            ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
            
            // Replenish permits every second
            scheduler.scheduleAtFixedRate(() -> {
                int permitsToRelease = maxOpsPerSecond - permits.availablePermits();
                if (permitsToRelease > 0) {
                    permits.release(permitsToRelease);
                }
            }, 1, 1, TimeUnit.SECONDS);
        }
        
        public void acquire(int numOps) throws InterruptedException {
            if (permits == null) {
                initialize();
            }
            permits.acquire(Math.min(numOps, maxOpsPerSecond));
        }

    }
}
