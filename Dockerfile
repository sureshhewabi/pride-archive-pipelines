# Build stage
FROM maven:3.3.9-jdk-8-alpine AS build-env

# Create app directory
WORKDIR /pride-pipelines

COPY src ./src
COPY pom.xml ./
RUN mvn clean package -DskipTests

# Package stage
FROM maven:3.3.9-jdk-8-alpine
WORKDIR /pride-pipelines
COPY --from=build-env /pride-pipelines/target/pride-pipelines.jar ./
COPY config/logback-spring.xml ./config/logback-spring.xml
ENTRYPOINT java ${JAVA_OPTS} -jar pride-pipelines.jar --spring.batch.job.names=${JOB_NAME}