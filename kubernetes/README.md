# Kubernetes Configurations for Movies Rating Project

This directory contains Kubernetes configuration files for deploying the Movies Rating project on a Kubernetes cluster. The configurations are organized by service type.

## Directory Structure

```
kubernetes/
├── application/
│   └── real-time-service-deployment.yaml
├── flink/
│   ├── flink-jobmanager-deployment.yaml
│   └── flink-taskmanager-deployment.yaml
├── hadoop/
│   ├── hadoop-datanode-deployment.yaml
│   └── hadoop-namenode-deployment.yaml
├── kafka/
│   └── kafka-statefulset.yaml
├── mongodb/
│   └── mongodb-statefulset.yaml
├── redis/
│   ├── redis-deployment.yaml
│   └── redis-init-job.yaml
└── spark/
    ├── spark-master-deployment.yaml
    └── spark-worker-deployment.yaml
```

## Services Overview

### Spark
- **spark-master**: Spark master node with web UI exposed on port 8080 and Spark master port 7077
- **spark-worker**: 3 Spark worker nodes that connect to the Spark master

### Flink
- **flink-jobmanager**: Flink JobManager with web UI exposed on port 8081
- **flink-taskmanager**: Flink TaskManager that connects to the JobManager

### Hadoop
- **namenode**: Hadoop NameNode with web UI exposed on port 9870 and HDFS port 8020
- **datanode**: Hadoop DataNode that connects to the NameNode

### Kafka
- **kafka**: Kafka broker with internal port 9092 and external port 9094

### Redis
- **redis**: Redis server with Redis port 6379 and Redis UI port 8001
- **redis-init**: Job that initializes Redis with vector indices

### MongoDB
- **mongodb**: MongoDB server with MongoDB port 27017

### Application
- **real-time-service**: Service that submits a Flink job to the JobManager

## Deployment Instructions

### Prerequisites
- Kubernetes cluster (e.g., Minikube, kind, or a cloud-based Kubernetes service)
- kubectl command-line tool
- Docker (for building the real-time-service image)

### Deployment Steps

1. **Create the namespace (optional)**:
   ```bash
   kubectl create namespace movies-rating
   ```

2. **Deploy Hadoop services**:
   ```bash
   kubectl apply -f hadoop/
   ```

3. **Deploy Spark services**:
   ```bash
   kubectl apply -f spark/
   ```

4. **Deploy Kafka**:
   ```bash
   kubectl apply -f kafka/
   ```

5. **Deploy Redis**:
   ```bash
   kubectl apply -f redis/
   ```

6. **Deploy MongoDB**:
   ```bash
   kubectl apply -f mongodb/
   ```

7. **Deploy Flink services**:
   ```bash
   kubectl apply -f flink/
   ```

8. **Build and deploy the real-time service**:
   ```bash
   # Update the path in real-time-service-deployment.yaml to point to your project root
   kubectl apply -f application/
   ```

9. **Initialize Redis**:
   ```bash
   kubectl apply -f redis/redis-init-job.yaml
   ```

### Accessing Services

- **Spark UI**: `kubectl port-forward svc/spark-master 8080:8080`
- **Flink UI**: `kubectl port-forward svc/flink-jobmanager 8081:8081`
- **Hadoop UI**: `kubectl port-forward svc/namenode 9870:9870`
- **Redis UI**: `kubectl port-forward svc/redis 8001:8001`

## Notes

- The configurations use PersistentVolumeClaims for data storage. Depending on your Kubernetes environment, you may need to provision PersistentVolumes or use a StorageClass.
- The real-time-service build job requires access to the Docker daemon. This may not work in all Kubernetes environments. You may need to build the image separately and push it to a container registry.
- For production use, consider adding resource limits, security contexts, and other production-ready configurations.