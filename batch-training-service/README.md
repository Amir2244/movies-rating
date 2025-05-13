# Batch Training Service

This service is responsible for training the ALS recommendation model using Apache Spark.

## Overview

The Batch Training Service follows a hexagonal architecture pattern and provides:

1. Model training using Apache Spark's ALS (Alternating Least Squares) algorithm
2. Model persistence to HDFS
3. Factor persistence to Redis
4. Model prediction capabilities for testing

## Running the Service

The service can be run in two modes:

### 1. Training Mode

This mode trains the ALS model using the configured data source and saves the model to HDFS.

```bash
java -jar batch-training-service.jar train
```

### 2. Prediction Mode

This mode loads a trained model from HDFS and makes predictions for test data.

```bash
java -jar batch-training-service.jar predict
```

#### Prediction Mode Options

The prediction mode supports several command-line options:

- `--model-path=<path>`: Specify a custom path to load the model from
- `--test-file=<path>`: Specify a CSV file containing test data

Example:

```bash
java -jar batch-training-service.jar predict --model-path=hdfs://namenode:8020/custom/model/path --test-file=/path/to/test_data.csv
```

## Test Data Format

When using a custom test file, it should be a CSV file with the following format:

```
userId,movieId
1,100
2,200
3,300
```

The first line can be a header (will be automatically detected and skipped).

## Docker Deployment

The service can be deployed using Docker and the provided docker-compose.yml file:

```bash
docker-compose up -d
```

To run the service in a specific mode within Docker:

```bash
docker exec -it batch-training-service java -jar /app/batch-training-service.jar train
docker exec -it batch-training-service java -jar /app/batch-training-service.jar predict
```

## Configuration

Configuration is managed through the `batch_config.properties` file. Key configuration options include:

- Spark configuration
- HDFS paths
- Redis connection details
- ALS model hyperparameters

## Development

### Building the Service

```bash
mvn clean package
```

This will create a JAR file with all dependencies included.

### Running Tests

```bash
mvn test
```