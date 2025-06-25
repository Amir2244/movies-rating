# Use an official Flink image as a base.
FROM flink:1.17.1-scala_2.12-java11

# Switch to the root user to be able to install packages
USER root

# Update package lists and install netcat (nc).
# We also clean up the apt cache to keep the image size down.
RUN apt-get update && \
    apt-get install -y netcat-openbsd && \
    rm -rf /var/lib/apt/lists/*

# Switch back to the default flink user
USER flink

# Copy the pre-built JAR file to Flink's usrlib directory
# This command assumes the JAR is in the 'target' directory when you run docker-compose build
COPY target/real-time-service-0.0.1.jar /opt/flink/usrlib/real-time-service.jar