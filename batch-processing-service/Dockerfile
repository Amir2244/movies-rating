# Use an official Spark image as a base.
FROM bde2020/spark-master

# Switch to the root user to be able to install packages if needed
USER root

# Update package lists and install netcat (nc).
# We use apk for Alpine Linux instead of apt-get
RUN apk update && \
    apk add --no-cache netcat-openbsd || apk add --no-cache netcat && \
    rm -rf /var/cache/apk/*

# Copy the pre-built JAR file to Spark's applications directory
# This command assumes the JAR is in the 'target' directory when you run docker-compose build
COPY target/batch-processing-service-0.0.1.jar /opt/spark-apps/batch-processing-service.jar

# Switch back to the default user if needed
# USER someuser
