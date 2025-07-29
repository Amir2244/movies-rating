# Movies Rating System

A comprehensive movie recommendation system that processes user interactions, generates personalized recommendations, and provides analytics insights.

## Overview

The Movies Rating System is a microservices-based application that implements a complete movie recommendation platform. It processes user interactions with movies (views, ratings, likes), generates personalized recommendations using collaborative filtering, and provides analytics insights through interactive dashboards.

## Architecture

The system follows a microservices architecture with several interconnected components that work together to provide a complete movie recommendation platform. Here's how the system is organized:

### Service Layers and Data Flow

1. **Frontend Layer**
   - The **Analytics UI** (built with Next.js) provides a user-friendly dashboard for visualizing analytics data. It communicates directly with the Recommendations API to display personalized recommendations.

2. **API Layer**
   - The **Recommendations API** (Spring Boot) serves as the gateway for recommendation requests, providing endpoints for accessing personalized movie recommendations.
   - The **Analytics API** (Spring Boot) offers endpoints for querying analytics data, supporting filtering, sorting, and pagination.

3. **Processing Layer**
   - The **Batch Processing Service** (Google Dataproc) trains recommendation models using historical data, generates user and item factors, and produces analytics insights. It sends data to the Analytics API and stores information in Redis.
   - The **Real-Time Service** (Google Dataproc) processes user interaction events in real-time, performing immediate analysis and generating recommendations. It communicates exclusively with Kafka for event processing.

4. **Data Storage Layer**
   - **MongoDB** stores analytics data and is accessed by the Analytics API.
   - **Redis** serves as a vector search database for fast similarity lookups, used by both the Batch Processing Service and Real-Time Service.
   - **Google Cloud Storage** stores training data and trained models, primarily used by the Batch Processing Service.

5. **Messaging Layer**
   - **Kafka** serves as the central event streaming platform, receiving events from various sources and delivering them to the Real-Time Service. It also sends data to Google Cloud Storage for long-term storage.

### Key Interactions

- User interactions flow through Kafka to the Real-Time Service for immediate processing
- The Batch Processing Service periodically trains models using historical data from Google Cloud Storage
- The Analytics API retrieves processed data from MongoDB to serve to the Analytics UI
- The Recommendations API provides personalized recommendations based on pre-computed data

All services share a common domain model through the Shared Kernel module and follow hexagonal architecture principles for clean separation of concerns.

## Services

### Core Services

#### Batch Processing Service
- Trains recommendation models using Google Dataproc's ALS algorithm
- Processes historical user ratings to generate user and item factors
- Stores trained models in Google Cloud Storage and factors in Redis
- Generates analytics data for insights

#### Real-Time Service
- Processes user interaction events in real-time using Google Dataproc
- Performs vector similarity search in Redis to find similar movies
- Generates personalized recommendations based on user interactions
- Notifies users of new recommendations

#### Recommendations API
- Provides REST endpoints for accessing personalized recommendations
- Retrieves pre-computed recommendations from MongoDB
- Supports filtering and pagination of recommendations
- Implements comprehensive error handling and validation

#### Analytics API
- Offers REST endpoints for querying analytics data
- Supports filtering by type, date range, and keywords
- Provides summary statistics and aggregated metrics
- Implements flexible sorting and pagination

#### Analytics UI
- Web-based dashboard for visualizing analytics data
- Interactive charts and graphs for various metrics
- Real-time updates of analytics information
- Responsive design for desktop and mobile devices

### Supporting Components

#### Shared Kernel
- Core library with domain models and utilities
- Ensures consistency across services
- Implements value objects for IDs and events
- Provides vector serialization utilities

## Technology Stack

### Backend
- **Java 17**: Core programming language
- **Spring Boot**: Web framework for APIs
- **Google Dataproc**: Managed service for batch and stream processing
- **Apache Kafka**: Event streaming platform
- **Redis**: In-memory database with vector search capabilities
- **MongoDB**: Document database for analytics and recommendations
- **Google Cloud Storage**: Cloud storage for model storage

### Frontend
- **Next.js**: React framework for the Analytics UI
- **React**: UI library
- **Tailwind CSS**: Utility-first CSS framework

### Infrastructure
- **Docker**: Containerization
- **Docker Compose**: Multi-container orchestration

## Getting Started

### Prerequisites

- Docker and Docker Compose installed on your machine
- Java 17 or higher
- Maven 3.6+
- Node.js 18+ (for Analytics UI development)

### Building the Services

1. **Build the shared-kernel module first**:
```bash
cd shared-kernel
mvn clean install
```

2. **Build the batch-processing-service**:
```bash
cd ../batch-processing-service
mvn clean package
```

3. **Build other services as needed**:
```bash
cd ../real-time-service
mvn clean package

cd ../recommendations-api
mvn clean package

cd ../analytics-api
mvn clean package
```

4. **Build the Analytics UI**:
```bash
cd ../analytics-ui
npm install
npm run build
```

### Starting the Services

To start all the services defined in the docker-compose.yml file:

```bash
docker-compose up -d
```

This will start the following services:
- Kafka
- Redis with RediSearch
- MongoDB
- All application services

Note: Hadoop, Spark, and Flink services have been migrated to Google Dataproc and are no longer started locally.

### Accessing Service UIs

- **Google Cloud Console (Dataproc)**: https://console.cloud.google.com/dataproc
- **Redis Insight**: http://localhost:8001
- **Analytics UI**: http://localhost:3000
- **Recommendations API**: http://localhost:8080/api/v1
- **Analytics API**: http://localhost:8083/api/v1

Note: HDFS, Spark, and Flink UIs are now accessible through the Google Cloud Console Dataproc section.

### Loading Data

Before running the application, you need to load your ratings data into Google Cloud Storage:

```bash
# Install Google Cloud SDK if you haven't already
# https://cloud.google.com/sdk/docs/install

# Authenticate with Google Cloud
gcloud auth login

# Set your project ID
gcloud config set project your-project-id

# Upload your ratings file to Google Cloud Storage
gsutil cp /path/to/your/ratings.csv gs://your-bucket/movielens/
```

### Running the Batch Processing

```bash
# Submit the batch processing job to Google Dataproc
gcloud dataproc jobs submit spark \
  --region=your-region \
  --cluster=your-cluster-name \
  --class=org.hiast.batch.adapter.driver.spark.BatchTrainingJob \
  --jars=gs://your-bucket/jars/batch-training-service.jar \
  -- arg1 arg2  # Optional arguments for your job
```

### Stopping the Services

To stop all services:

```bash
docker-compose down
```

To stop all services and remove volumes (this will delete all data):

```bash
docker-compose down -v
```

## Development

### Project Structure

```
movies-rating/
├── shared-kernel/              # Shared domain models and utilities
├── batch-processing-service/   # Batch training with Spark
├── real-time-service/          # Real-time processing with Flink
├── recommendations-api/        # REST API for recommendations
├── analytics-api/              # REST API for analytics data
├── analytics-ui/               # Web dashboard for analytics
├── docker-compose.yml          # Docker Compose configuration
└── README.md                   # This file
```

### Development Guidelines

1. **Hexagonal Architecture**: All services follow hexagonal architecture principles with clear separation between domain, application, and infrastructure layers.

2. **Domain-Driven Design**: Use the shared domain models from the shared-kernel module to ensure consistency across services.

3. **Testing**: Write comprehensive unit and integration tests for all services.

4. **Documentation**: Keep README files and API documentation up-to-date.

5. **Code Quality**: Follow SOLID principles and maintain clean code practices.

## Testing

### K6 Load Testing

The project includes k6 load testing scripts for the Recommendations API. These tests are located in the `k6-tests` directory and are designed to validate the functionality and performance of the recommendations API.

#### Test Types

1. **Basic Functionality Tests**: Verify that the API works correctly with different user IDs and limits.
2. **Load Tests**: Simulate moderate traffic to evaluate performance under normal conditions.
3. **Stress Tests**: Simulate heavy traffic to evaluate performance under high load.

#### Running the Tests

To run the k6 tests, you need to have k6 installed on your system. See the [k6-tests/README.md](k6-tests/README.md) for detailed instructions on running the tests and interpreting the results.

```bash
# Run basic functionality test
k6 run k6-tests/scripts/recommendations-api-test.js

# Run load test
k6 run k6-tests/scripts/recommendations-api-load-test.js

# Run stress test
k6 run k6-tests/scripts/recommendations-api-stress-test.js
```

## CI/CD Pipeline

The project includes a Jenkins pipeline (defined in `Jenkinsfile`) that automates the build, test, and deployment process.

### Pipeline Stages

1. **Checkout**: Retrieves the source code from the repository
2. **Build Shared Kernel**: Builds the shared domain models and utilities
3. **Build Services**: Builds all service components in parallel
4. **Run Tests**: Executes tests for all services in parallel
5. **Build Docker Images**: Creates Docker images for all services
6. **Push Docker Images**: Pushes the images to Docker Hub
7. **Prepare Docker Volumes**: Ensures all necessary Docker volumes exist

### Docker Volumes Management

The system uses several Docker volumes for persistent data storage:

- **redis_data**: Stores Redis data including vector indexes
- **mongo_data**: Stores MongoDB data
- **kafka_data**: Stores Kafka logs and data

These volumes are critical for data persistence across container restarts and deployments. The CI/CD pipeline:

1. Creates the necessary volume directories if they don't exist
2. Ensures Docker volumes are properly configured
3. Preserves volumes during cleanup to maintain data integrity

While the batch-processing-service and real-time-service don't directly use volumes in their Dockerfiles, they depend on services that do use volumes (like Redis, Kafka). The pipeline ensures these dependencies have their volumes properly managed.

Note: HDFS volumes (namenode_data, datanode_data) are no longer used as these services have been migrated to Google Cloud Storage.

### Volume Backup Considerations

For production environments, consider implementing a backup strategy for these volumes:

```bash
# Example backup command for a Docker volume
docker run --rm -v redis_data:/source -v /path/on/host:/backup alpine tar -czf /backup/redis_backup.tar.gz -C /source .
```

For Google Cloud Storage, use the following backup strategy:

```bash
# Example backup command for Google Cloud Storage
gsutil -m cp -r gs://your-bucket/movielens gs://your-backup-bucket/movielens-backup
```

## Kubernetes Deployment

The Movies Rating System can be deployed to a Kubernetes cluster. The Kubernetes manifests are provided in the `/kubernetes` directory.

### Prerequisites

- A running Kubernetes cluster (e.g., Minikube, Docker Desktop Kubernetes, or a cloud provider's cluster)
- `kubectl` installed and configured to connect to your cluster
- Container registry access for storing Docker images

### Deployment Steps

1. **Create Namespace (if not exists):**
   ```bash
   kubectl create namespace movies-rating
   ```

2. **Apply Storage Resources:**
   ```bash
   kubectl apply -f kubernetes/kafka-data-persistentvolumeclaim.yaml
   ```

3. **Deploy Infrastructure Services:**
   ```bash
   # Deploy MongoDB
   kubectl apply -f kubernetes/mongodb-deployment.yaml
   kubectl apply -f kubernetes/mongodb-service.yaml

   # Deploy Redis
   kubectl apply -f kubernetes/redis-pvc.yaml
   kubectl apply -f kubernetes/redis-deployment.yaml
   kubectl apply -f kubernetes/redis-service.yaml
   kubectl apply -f kubernetes/redis-init-cm0-configmap.yaml
   kubectl apply -f kubernetes/redis-init-pod.yaml
   kubectl apply -f kubernetes/redis-init-service.yaml

   # Deploy Kafka
   kubectl apply -f kubernetes/kafka-data-persistentvolumeclaim.yaml
   kubectl apply -f kubernetes/kafka-deployment.yaml
   kubectl apply -f kubernetes/kafka-headless-service.yaml
   kubectl apply -f kubernetes/kafka-external-service.yaml
   ```

4. **Set up Google Dataproc:**
   ```bash
   # Create a Dataproc cluster for batch processing
   gcloud dataproc clusters create batch-cluster \
     --region=your-region \
     --zone=your-zone \
     --master-machine-type=n1-standard-4 \
     --master-boot-disk-size=500 \
     --num-workers=2 \
     --worker-machine-type=n1-standard-4 \
     --worker-boot-disk-size=500 \
     --image-version=2.0-debian10

   # Create a Dataproc cluster for real-time processing
   gcloud dataproc clusters create realtime-cluster \
     --region=your-region \
     --zone=your-zone \
     --master-machine-type=n1-standard-4 \
     --master-boot-disk-size=500 \
     --num-workers=2 \
     --worker-machine-type=n1-standard-4 \
     --worker-boot-disk-size=500 \
     --image-version=2.0-debian10 \
     --properties=spark:spark.dynamicAllocation.enabled=false
   ```

5. **Deploy Application Services:**
   ```bash
   # Deploy APIs
   kubectl apply -f kubernetes/recommendations-api-deployment.yaml
   kubectl apply -f kubernetes/recommendations-api-service.yaml
   kubectl apply -f kubernetes/analytics-api-deployment.yaml
   kubectl apply -f kubernetes/analytics-api-service.yaml
   ```

### Service Access

The services are exposed as NodePort services. To access them:

1. Get the services and their ports:
   ```bash
   kubectl get services -n movies-rating
   ```

2. Access the services using your cluster's node IP and the assigned NodePort

### Scaling Services

You can scale the deployments based on your workload requirements:

```bash
# Scale Google Dataproc clusters
gcloud dataproc clusters update batch-cluster \
  --region=your-region \
  --num-workers=4

# Scale other Kubernetes deployments
kubectl scale deployment redis --replicas=3 -n movies-rating
kubectl scale deployment recommendations-api --replicas=3 -n movies-rating
```

### Monitoring

To monitor the pods and their status:

```bash
kubectl get pods -n movies-rating
kubectl describe pod <pod-name> -n movies-rating
kubectl logs <pod-name> -n movies-rating
```

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
