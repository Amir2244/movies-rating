# Docker Compose Setup for Movies Rating Application

This README provides instructions on how to use the Docker Compose setup for the Movies Rating application.

## Prerequisites

- Docker and Docker Compose installed on your machine
- Java 11 or higher
- Maven

## Building the Application

Before starting the Docker Compose services, you need to build the application JAR:

```bash
# Build the shared-kernel module first
cd shared-kernel
mvn clean install

# Build the batch-training-service
cd ../batch-training-service
mvn clean package
```

This will create the JAR file at `batch-training-service/target/batch-training-service-1.0-SNAPSHOT.jar`.

## Starting the Services

To start all the services defined in the docker-compose.yml file:

```bash
docker-compose up -d
```

This will start the following services:
- Hadoop Namenode (HDFS master)
- Hadoop Datanode (HDFS worker)
- Spark Master
- Spark Worker
- Redis

## Accessing Service UIs

- HDFS Web UI: http://localhost:9870
- Spark Master UI: http://localhost:8080
- Spark Worker UI: http://localhost:8081
- Datanode Web UI: http://localhost:9864

## Loading Data into HDFS

Before running the application, you need to load your ratings data into HDFS:

```bash
# Copy a local ratings file to the namenode container
docker cp /path/to/your/ratings.csv namenode:/tmp/

# Execute HDFS commands inside the namenode container to create directories and copy the file
docker exec -it namenode bash
hadoop fs -mkdir -p /user/your_user/movielens
hadoop fs -put /tmp/ratings.csv /user/your_user/movielens/
```

## Updating Configuration

A Docker-specific configuration file has been provided at `batch-training-service/src/main/resources/batch_config.properties.docker`. You can use this file by copying it to the main configuration location:

```bash
# Copy the Docker configuration file to the main configuration location
cp batch-training-service/src/main/resources/batch_config.properties.docker batch-training-service/src/main/resources/batch_config.properties
```

The Docker configuration file contains the following settings:

```properties
# Spark master URL points to the Spark master container
spark.master.url=spark://spark-master:7077

# HDFS path points to the namenode container
data.input.ratings.hdfs.path=hdfs://namenode:8020/input/ratings.csv

# Redis host points to the Redis container
redis.host=redis
redis.port=6379
```

You may need to adjust the HDFS path to match where you uploaded your ratings file.

## Running the Application

You can run the application in two ways:

### 1. Using spark-submit inside the Spark Master container

```bash
docker exec -it spark-master bash
cd /opt/spark-apps
spark-submit --class org.hiast.batch.adapter.driver.spark.BatchTrainingJob batch-training-service.jar
```

### 2. Using the Spark REST API

```bash
curl -X POST http://localhost:6066/v1/submissions/create --header "Content-Type:application/json;charset=UTF-8" --data '{
  "action": "CreateSubmissionRequest",
  "appResource": "/opt/spark-apps/batch-training-service.jar",
  "appArgs": [],
  "clientSparkVersion": "3.3.0",
  "mainClass": "org.hiast.batch.adapter.driver.spark.BatchTrainingJob",
  "environmentVariables": {
    "SPARK_ENV_LOADED": "1"
  },
  "sparkProperties": {
    "spark.master": "spark://spark-master:7077",
    "spark.app.name": "MovieRecsysBatchTrainerHDFS",
    "spark.submit.deployMode": "cluster",
    "spark.driver.supervise": "false"
  }
}'
```

## Stopping the Services

To stop all services:

```bash
docker-compose down
```

To stop all services and remove volumes (this will delete all data):

```bash
docker-compose down -v
```
