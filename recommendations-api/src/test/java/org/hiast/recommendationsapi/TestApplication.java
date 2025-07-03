package org.hiast.recommendationsapi;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.kafka.KafkaAutoConfiguration;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.data.mongodb.config.EnableMongoAuditing;

/**
 * Test-specific Spring Boot application class that excludes Kafka-related components.
 * This class is used only during tests to avoid connecting to Kafka.
 */
@SpringBootApplication(exclude = KafkaAutoConfiguration.class)
@EnableConfigurationProperties
@EnableMongoAuditing
@ComponentScan(
    basePackages = "org.hiast.recommendationsapi",
    excludeFilters = @ComponentScan.Filter(
        type = FilterType.REGEX,
        pattern = "org\\.hiast\\.recommendationsapi\\.adapter\\.out\\.messaging\\.kafka\\..*"
    )
)
public class TestApplication {

    public static void main(String[] args) {
        SpringApplication.run(TestApplication.class, args);
    }
}