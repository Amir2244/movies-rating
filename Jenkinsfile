pipeline {
    agent any

    tools {
        maven 'Maven-3.9'
    }

    environment {
        DOCKERHUB_CREDENTIALS = credentials('dockerhublogin')
        VERSION_TAG = "${BUILD_NUMBER}-${GIT_COMMIT.take(7)}"
    }

    stages {
        stage('Checkout') {
            steps {
                checkout scm
            }
        }

        stage('Build Shared Kernel') {
            steps {
                dir('shared-kernel') {
                    sh 'mvn clean install'
                }
            }
        }

        stage('Build Services') {
            steps {
                parallel(
                    'Analytics API': {
                    dir('analytics-api') {
                        sh 'mvn clean package -DskipTests'
                        }
                    },
                    'Recommendations API': {
                    dir('recommendations-api') {
                        sh 'mvn clean package -DskipTests'
                        }
                    },
                    'Batch Processing Service': {
                    dir('batch-processing-service') {
                        sh 'mvn clean package -DskipTests'
                        }
                    },
                    'Real-Time Service': {
                    dir('real-time-service') {
                        sh 'mvn clean package -DskipTests'
                        }
                    },
                    'Analytics UI': {
                    dir('analytics-ui') {
                        sh 'npm ci'
                            sh 'npm run build'
                        }
                    }
                )
            }
        }

        stage('Run Tests') {
            steps {
                parallel(
                    'Analytics API Tests': {
                    dir('analytics-api') {
                        sh 'mvn test'
                        }
                    },
                    'Recommendations API Tests': {
                    dir('recommendations-api') {
                        sh 'mvn test'
                        }
                    },
                    'Batch Processing Service Tests': {
                    dir('batch-processing-service') {
                        sh 'mvn test'
                        }
                    }
                )
            }
            post {
                always {
                    junit '**/target/surefire-reports/*.xml'
                }
            }
        }

        stage('Build and Push Docker Images') {
            steps {
                script {
                    docker.withRegistry('https://index.docker.io/v1/', 'dockerhublogin') {
                        def services = [
                            'analytics-api',
                            'recommendations-api',
                            'batch-processing-service',
                            'real-time-service',
                            'analytics-ui'
                        ]

                        def builds = services.collectEntries { serviceName ->
                            ["Build & Push ${serviceName}": {
                            try {
                                def imageName = "${DOCKERHUB_CREDENTIALS_USR}/movies-rating-${serviceName}"

                                    def dockerImage = docker.build("${imageName}:${VERSION_TAG}", "./${serviceName}")

                                    echo "Pushing image ${imageName}:${VERSION_TAG}"
                                    dockerImage.push()

                                    echo "Tagging and pushing image ${imageName}:latest"
                                    dockerImage.push('latest')

                                    echo "✅ Successfully built and pushed ${imageName}"

                                } catch (Exception e) {
                                echo "❌ Error processing Docker image for ${serviceName}: ${e.getMessage()}"
                                    error "Failed to build or push Docker image for ${serviceName}"
                                }
                            }]
                        }
                        parallel builds
                    }
                }
            }
        }

        stage('Deploy to GKE') {
            environment {
                GCP_PROJECT_ID = 'stoked-mapper-461613-k5'
                GKE_CLUSTER_NAME = 'jenkins-cd'
                GKE_CLUSTER_ZONE = 'us-east1-d'
            }
            steps {
                script {
                    withCredentials([file(credentialsId: 'gcp-service-account-key', variable: 'GCP_SA_KEY_PATH')]) {
                         echo "Authenticating with GCP..."
                              sh 'gcloud auth activate-service-account --key-file=$GCP_SA_KEY_PATH'
                              sh 'gcloud config set project $GCP_PROJECT_ID'
                              echo "Configuring kubectl for GKE cluster ${GKE_CLUSTER_NAME}..."
                              sh 'gcloud container clusters get-credentials $GKE_CLUSTER_NAME --zone $GKE_CLUSTER_ZONE'
                              echo "Deploying application resources from 'k8s' directory..."
                              sh 'kubectl apply -f kubernetes/'

                        def services = [
                            'analytics-api',
                            'recommendations-api',
                            'batch-processing-service',
                            'real-time-service',
                            'analytics-ui'
                        ]

                        echo "Updating deployment images to version: ${VERSION_TAG}"
                        services.each { serviceName ->
                            def deploymentName = "movies-rating-${serviceName}"
                            def imageName = "${DOCKERHUB_CREDENTIALS_USR}/movies-rating-${serviceName}:${VERSION_TAG}"
                            sh "kubectl set image deployment/${deploymentName} ${serviceName}=${imageName} --record"
                        }
                    }
                }
            }
        }

        stage('Prepare Docker Volumes') {
            steps {
                script {
                    echo 'Ensuring Docker volumes exist for persistent data storage...'
                    def volumes = ['namenode_data', 'datanode_data', 'redis_data', 'mongo_data', 'kafka_data']

                    volumes.each { volumeName ->
                        sh "docker volume ls -q -f name=${volumeName} | grep -q . || docker volume create ${volumeName}"
                    }
                    echo 'Docker volumes prepared successfully.'
                }
            }
        }
    }

    post {
        always {
            script {
                echo '🧹 Cleaning up Docker resources...'
                sh 'docker image prune -f'
                sh 'docker container prune -f'
                echo 'Docker cleanup completed. Volumes are preserved.'
            }
        }
        success {
            echo '🚀 Pipeline completed successfully!'
            echo "Docker images tagged with 'latest' and '${VERSION_TAG}' have been pushed to Docker Hub."
        }
        failure {
            echo '🛑 Pipeline failed! Check the logs for details.'
        }
    }
}