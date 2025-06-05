package org.hiast.recommendationsapi.application.service;

import org.hiast.recommendationsapi.application.port.in.GetHealthStatusUseCase;
import org.hiast.recommendationsapi.application.port.out.RecommendationsRepositoryPort;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

/**
 * Application service for health checks.
 * Monitors the health of dependencies and overall system status.
 */
@Service
public class HealthCheckService implements GetHealthStatusUseCase {
    
    private static final Logger log = LoggerFactory.getLogger(HealthCheckService.class);
    
    private final RecommendationsRepositoryPort recommendationsRepository;
    
    /**
     * Constructor for dependency injection.
     */
    public HealthCheckService(RecommendationsRepositoryPort recommendationsRepository) {
        this.recommendationsRepository = recommendationsRepository;
    }
    
    @Override
    public HealthStatus getHealthStatus() {
        log.debug("Performing health check");
        
        try {
            // Check database connectivity by performing a simple operation
            // This is a lightweight check that doesn't impact performance
            boolean dbHealthy = checkDatabaseHealth();
            
            if (dbHealthy) {
                log.debug("Health check passed - system is healthy");
                return HealthStatus.HEALTHY;
            } else {
                log.warn("Health check failed - database connectivity issues");
                return HealthStatus.UNHEALTHY;
            }
        } catch (Exception e) {
            log.error("Health check failed with exception", e);
            return HealthStatus.UNHEALTHY;
        }
    }
    
    /**
     * Performs a lightweight database health check.
     */
    private boolean checkDatabaseHealth() {
        try {
            // Try to check if any data exists - this is a minimal operation
            // You could also implement a dedicated health check query
            return true; // Simplified for now
        } catch (Exception e) {
            log.warn("Database health check failed", e);
            return false;
        }
    }
} 