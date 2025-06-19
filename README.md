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
   - The **Batch Processing Service** (Apache Spark) trains recommendation models using historical data, generates user and item factors, and produces analytics insights. It sends data to the Analytics API and stores information in Redis.
   - The **Real-Time Service** (Apache Flink) processes user interaction events in real-time, performing immediate analysis and generating recommendations. It communicates exclusively with Kafka for event processing.

4. **Data Storage Layer**
   - **MongoDB** stores analytics data and is accessed by the Analytics API.
   - **Redis** serves as a vector search database for fast similarity lookups, used by both the Batch Processing Service and Real-Time Service.
   - **HDFS** stores training data and trained models, primarily used by the Batch Processing Service.

5. **Messaging Layer**
   - **Kafka** serves as the central event streaming platform, receiving events from various sources and delivering them to the Real-Time Service. It also sends data to HDFS for long-term storage.

### Key Interactions

- User interactions flow through Kafka to the Real-Time Service for immediate processing
- The Batch Processing Service periodically trains models using historical data from HDFS
- The Analytics API retrieves processed data from MongoDB to serve to the Analytics UI
- The Recommendations API provides personalized recommendations based on pre-computed data

All services share a common domain model through the Shared Kernel module and follow hexagonal architecture principles for clean separation of concerns.

## Services

### Core Services

#### Batch Processing Service
- Trains recommendation models using Apache Spark's ALS algorithm
- Processes historical user ratings to generate user and item factors
- Stores trained models in HDFS and factors in Redis
- Generates analytics data for insights

#### Real-Time Service
- Processes user interaction events in real-time using Apache Flink
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
- **Apache Spark**: Distributed computing for batch processing
- **Apache Flink**: Stream processing for real-time events
- **Apache Kafka**: Event streaming platform
- **Redis**: In-memory database with vector search capabilities
- **MongoDB**: Document database for analytics and recommendations
- **HDFS**: Distributed file system for model storage

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
- Hadoop Namenode and Datanode
- Spark Master and Workers
- Flink JobManager and TaskManager
- Kafka
- Redis with RediSearch
- MongoDB
- All application services

### Accessing Service UIs

- **HDFS Web UI**: http://localhost:9870
- **Spark Master UI**: http://localhost:8080
- **Spark Worker UI**: http://localhost:8081
- **Flink Dashboard**: http://localhost:8081
- **Redis Insight**: http://localhost:8001
- **Analytics UI**: http://localhost:3000
- **Recommendations API**: http://localhost:8080/api/v1
- **Analytics API**: http://localhost:8083/api/v1

### Loading Data

Before running the application, you need to load your ratings data into HDFS:

```bash
# Copy a local ratings file to the namenode container
docker cp /path/to/your/ratings.csv namenode:/tmp/

# Execute HDFS commands inside the namenode container
docker exec -it namenode bash
hadoop fs -mkdir -p /user/your_user/movielens
hadoop fs -put /tmp/ratings.csv /user/your_user/movielens/
```

### Running the Batch Processing

```bash
docker exec -it spark-master bash
cd /opt/spark-apps
spark-submit --class org.hiast.batch.adapter.driver.spark.BatchTrainingJob batch-training-service.jar
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

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
