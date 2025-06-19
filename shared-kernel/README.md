# Shared Kernel

## Overview
The Shared Kernel is a core library module that contains domain models, value objects, and utilities shared across multiple services in the Movies Rating application. It ensures consistency in domain concepts and reduces duplication across services.

## Purpose
- Provide a single source of truth for domain models
- Ensure consistent handling of IDs and value objects
- Share common utilities and exceptions
- Define events for inter-service communication

## Components

### Domain Models
The shared kernel contains several domain models used across services:

#### Core Models
- `MovieRecommendation`: Represents a movie recommendation with predicted rating
- `UserRecommendations`: Collection of recommendations for a specific user
- `MovieMetaData`: Metadata about movies
- `RatingValue`: Value object for movie ratings
- `InteractionEventDetails`: Details of user interaction events
- `DataAnalytics`: Model for analytics data

#### Analytics
- `AnalyticsType`: Enum defining different types of analytics data
- Additional analytics-specific models in the `analytics` package

#### Factors
- Factor models used in the recommendation algorithms
- Vector representations of users and items

### ID Value Objects
- `UserId`: Value object for user identifiers
- `MovieId`: Value object for movie identifiers

### Events
- `EventType`: Enum defining different types of events in the system

### Utilities
- `VectorSerializationUtil`: Utilities for serializing and deserializing vectors
- `VectorMetadata`: Metadata for vectors used in recommendations
- `VectorSerializationException`: Exception for vector serialization errors

## Usage

### Maven Dependency
To use the Shared Kernel in another service, add the following dependency to your `pom.xml`:

```xml
<dependency>
    <groupId>org.hiast</groupId>
    <artifactId>shared-kernel</artifactId>
    <version>${project.version}</version>
</dependency>
```

### Building
To build the Shared Kernel:

```bash
mvn clean install
```

This will compile the code, run tests, and install the artifact in your local Maven repository.

## Design Principles

### Domain-Driven Design
The Shared Kernel follows Domain-Driven Design principles:
- Rich domain models with behavior
- Value objects for immutable concepts
- Clear boundaries between different contexts

### Immutability
Most classes in the Shared Kernel are designed to be immutable to ensure thread safety and prevent unexpected side effects.

### Minimal Dependencies
The Shared Kernel has minimal external dependencies to avoid version conflicts when used in different services.

## Integration with Services

### Real-Time Service
- Uses event models for processing user interactions
- Uses ID value objects for identifying users and movies
- Uses vector utilities for similarity search

### Batch Processing Service
- Uses domain models for storing recommendation results
- Uses ID value objects for identifying users and movies
- Uses vector utilities for model training

### Recommendations API
- Uses domain models for exposing recommendations via REST API
- Uses ID value objects for identifying users and movies

### Analytics API
- Uses analytics models for exposing analytics data via REST API
- Uses ID value objects for identifying users and movies

## Development Guidelines

### Adding New Models
When adding new models to the Shared Kernel:
1. Ensure they are truly shared across multiple services
2. Follow immutability principles
3. Include comprehensive JavaDoc
4. Add appropriate unit tests

### Versioning
Changes to the Shared Kernel should be carefully managed:
1. Avoid breaking changes when possible
2. Use semantic versioning
3. Communicate changes to all teams using the Shared Kernel