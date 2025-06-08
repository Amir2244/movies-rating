package org.hiast.recommendationsapi.application.port.in;

/**
 * Use case for checking the health status of the recommendations system.
 */
public interface GetHealthStatusUseCase {
    
    /**
     * Checks if the recommendations system is healthy and operational.
     *
     * @return HealthStatus indicating system health.
     */
    HealthStatus getHealthStatus();
    
    /**
     * Health status enumeration.
     */
    enum HealthStatus {
        HEALTHY("System is operational"),
        UNHEALTHY("System has issues"),
        DEGRADED("System is running with reduced functionality");
        
        private final String description;
        
        HealthStatus(String description) {
            this.description = description;
        }
        
        public String getDescription() {
            return description;
        }
    }
} 