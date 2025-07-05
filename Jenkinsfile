pipeline {
    agent any

    environment {
        DOCKERHUB_CREDENTIALS = credentials('dockerhublogin')
        VERSION_TAG = "${BUILD_NUMBER}-${GIT_COMMIT.take(7)}"
        maven 'Maven-3.9'
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
                    bat 'mvn clean install'
                }
            }
        }

        stage('Build Services') {
            parallel {
                stage('Analytics API') {
                    steps {
                        dir('analytics-api') {
                            bat 'mvn clean package -DskipTests'
                        }
                    }
                }

                stage('Recommendations API') {
                    steps {
                        dir('recommendations-api') {
                            bat 'mvn clean package -DskipTests'
                        }
                    }
                }

                stage('Batch Processing Service') {
                    steps {
                        dir('batch-processing-service') {
                            bat 'mvn clean package -DskipTests'
                        }
                    }
                }

                stage('Real-Time Service') {
                    steps {
                        dir('real-time-service') {
                            bat 'mvn clean package -DskipTests'
                        }
                    }
                }

                stage('Analytics UI') {
                    steps {
                        dir('analytics-ui') {
                            bat 'npm ci'
                            bat 'npm run build'
                        }
                    }
                }
            }
        }

        stage('Run Tests') {
            parallel {
                stage('Analytics API Tests') {
                    steps {
                        dir('analytics-api') {
                            bat 'mvn test'
                        }
                    }
                    post {
                        always {
                            junit '**/target/surefire-reports/*.xml'
                        }
                    }
                }

                stage('Recommendations API Tests') {
                    steps {
                        dir('recommendations-api') {
                            bat 'mvn test'
                        }
                    }
                    post {
                        always {
                            junit '**/target/surefire-reports/*.xml'
                        }
                    }
                }

                stage('Batch Processing Service Tests') {
                    steps {
                        dir('batch-processing-service') {
                            bat 'mvn test'
                        }
                    }
                    post {
                        always {
                            junit '**/target/surefire-reports/*.xml'
                        }
                    }
                }

                stage('Real-Time Service Tests') {
                    steps {
                        dir('real-time-service') {
                            bat 'mvn test'
                        }
                    }
                    post {
                        always {
                            junit '**/target/surefire-reports/*.xml'
                        }
                    }
                }
            }
        }

        stage('Build Docker Images') {
            steps {

                bat 'echo %DOCKERHUB_CREDENTIALS_PSW% | docker login -u %DOCKERHUB_CREDENTIALS_USR% --password-stdin'

                script {
                    try {
                        bat "docker build -t %DOCKERHUB_CREDENTIALS_USR%/movies-rating-analytics-api:latest -t %DOCKERHUB_CREDENTIALS_USR%/movies-rating-analytics-api:${VERSION_TAG} analytics-api"

                        bat "docker build -t %DOCKERHUB_CREDENTIALS_USR%/movies-rating-recommendations-api:latest -t %DOCKERHUB_CREDENTIALS_USR%/movies-rating-recommendations-api:${VERSION_TAG} recommendations-api"

                        bat "docker build -t %DOCKERHUB_CREDENTIALS_USR%/movies-rating-batch-processing:latest -t %DOCKERHUB_CREDENTIALS_USR%/movies-rating-batch-processing:${VERSION_TAG} batch-processing-service"

                        bat "docker build -t %DOCKERHUB_CREDENTIALS_USR%/movies-rating-real-time:latest -t %DOCKERHUB_CREDENTIALS_USR%/movies-rating-real-time:${VERSION_TAG} real-time-service"

                        bat "docker build -t %DOCKERHUB_CREDENTIALS_USR%/movies-rating-analytics-ui:latest -t %DOCKERHUB_CREDENTIALS_USR%/movies-rating-analytics-ui:${VERSION_TAG} analytics-ui"
                    } catch (Exception e) {
                        echo "Error building Docker images: ${e.getMessage()}"
                        error "Failed to build Docker images"
                    }
                }
            }
        }

        stage('Push Docker Images') {
            steps {
                script {
                    try {
                        bat 'docker push %DOCKERHUB_CREDENTIALS_USR%/movies-rating-analytics-api:latest'
                        bat "docker push %DOCKERHUB_CREDENTIALS_USR%/movies-rating-analytics-api:${VERSION_TAG}"

                        bat 'docker push %DOCKERHUB_CREDENTIALS_USR%/movies-rating-recommendations-api:latest'
                        bat "docker push %DOCKERHUB_CREDENTIALS_USR%/movies-rating-recommendations-api:${VERSION_TAG}"

                        bat 'docker push %DOCKERHUB_CREDENTIALS_USR%/movies-rating-batch-processing:latest'
                        bat "docker push %DOCKERHUB_CREDENTIALS_USR%/movies-rating-batch-processing:${VERSION_TAG}"

                        bat 'docker push %DOCKERHUB_CREDENTIALS_USR%/movies-rating-real-time:latest'
                        bat "docker push %DOCKERHUB_CREDENTIALS_USR%/movies-rating-real-time:${VERSION_TAG}"

                        bat 'docker push %DOCKERHUB_CREDENTIALS_USR%/movies-rating-analytics-ui:latest'
                        bat "docker push %DOCKERHUB_CREDENTIALS_USR%/movies-rating-analytics-ui:${VERSION_TAG}"

                        echo "Successfully pushed all Docker images to Docker Hub"
                    } catch (Exception e) {
                        echo "Error pushing Docker images: ${e.getMessage()}"
                        error "Failed to push Docker images"
                    }
                }
            }
        }

        stage('Prepare Docker Volumes') {
            steps {
                script {
                    try {
                        echo 'Ensuring Docker volumes exist for persistent data storage...'

                        // Create docker-volumes directory if it doesn't exist
                        bat 'if not exist docker-volumes mkdir docker-volumes'
                        bat 'if not exist docker-volumes\\namenode_data mkdir docker-volumes\\namenode_data'
                        bat 'if not exist docker-volumes\\datanode_data mkdir docker-volumes\\datanode_data'
                        bat 'if not exist docker-volumes\\redis_data mkdir docker-volumes\\redis_data'
                        bat 'if not exist docker-volumes\\mongo_data mkdir docker-volumes\\mongo_data'

                        // Check if Docker volumes exist, create them if they don't
                        bat 'docker volume ls | findstr "namenode_data" || docker volume create namenode_data'
                        bat 'docker volume ls | findstr "datanode_data" || docker volume create datanode_data'
                        bat 'docker volume ls | findstr "redis_data" || docker volume create redis_data'
                        bat 'docker volume ls | findstr "mongo_data" || docker volume create mongo_data'
                        bat 'docker volume ls | findstr "kafka_data" || docker volume create kafka_data'

                        echo 'Docker volumes prepared successfully'
                    } catch (Exception e) {
                        echo "Warning: Docker volume preparation failed: ${e.getMessage()}"
                        echo "This may cause issues with data persistence. Check volume configuration."
                    }
                }
            }
        }
    }

    post {
        always {
            bat 'docker logout'
            script {
                try {
                    echo 'Cleaning up Docker resources...'
                    bat 'docker image prune -f'
                    bat 'docker container prune -f'
                    echo 'Docker cleanup completed'
                    echo 'Preserving Docker volumes for data persistence'
                } catch (Exception e) {
                    echo "Warning: Docker cleanup failed: ${e.getMessage()}"
                }
            }
        }
        success {
            echo 'Pipeline completed successfully!'
            echo "Docker images tagged with 'latest' and '${VERSION_TAG}' have been pushed to Docker Hub"
            echo "Note: Docker volumes for services like namenode, datanode, redis, mongodb, and kafka are preserved for data persistence"
        }
        failure {
            echo 'Pipeline failed!'
            echo 'Check the logs above for more details on the failure'
        }
    }
}
