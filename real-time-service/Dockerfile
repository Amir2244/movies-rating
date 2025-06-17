# Use an official Flink image as a parent image
FROM flink:1.17.1-scala_2.12-java11

# The JAR file of our application
ARG JAR_FILE=target/real-time-service-1.0-SNAPSHOT.jar

# Copy the JAR file from the target directory to the Flink's lib directory
COPY ${JAR_FILE} /opt/flink/usrlib/real-time-service-1.0-SNAPSHOT.jar
