package org.hiast.recommendationsapi.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.EnableAspectJAutoProxy;

/**
 * Configuration for Aspect-Oriented Programming (AOP).
 * Enables AspectJ auto-proxying and defines monitoring properties.
 */
@Configuration
@EnableAspectJAutoProxy
@ConfigurationProperties(prefix = "app.monitoring")
public class AopConfig {
    
    private Performance performance = new Performance();
    
    public Performance getPerformance() {
        return performance;
    }
    
    public void setPerformance(Performance performance) {
        this.performance = performance;
    }
    
    public static class Performance {
        private boolean enabled = true;
        private long slowThresholdMs = 1000;
        
        public boolean isEnabled() {
            return enabled;
        }
        
        public void setEnabled(boolean enabled) {
            this.enabled = enabled;
        }
        
        public long getSlowThresholdMs() {
            return slowThresholdMs;
        }
        
        public void setSlowThresholdMs(long slowThresholdMs) {
            this.slowThresholdMs = slowThresholdMs;
        }
    }
}
