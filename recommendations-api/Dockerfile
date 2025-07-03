# Use OpenJDK 17 as the base image
FROM eclipse-temurin:17-jdk-jammy

# Set working directory
WORKDIR /app

# Add a volume pointing to /tmp for temporary files
VOLUME /tmp

# Make port 8082 available to the world outside this container
EXPOSE 8082

# Set the JAR file as an environment variable
ARG WAR_FILE=target/recommendations-api-0.0.1.war

# Add the application's JAR to the container
COPY ${WAR_FILE} app.war

# Set MongoDB connection environment variables with default values
# These can be overridden when running the container
ENV SPRING_DATA_MONGODB_HOST=localhost
ENV SPRING_DATA_MONGODB_PORT=27018
ENV SPRING_DATA_MONGODB_DATABASE=movie-recommendations

# Run the jar file
ENTRYPOINT ["java", "-jar", "/app/app.war"]