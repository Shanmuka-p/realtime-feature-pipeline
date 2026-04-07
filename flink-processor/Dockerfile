# Stage 1: Build the JAR using Maven
FROM maven:3.8.4-openjdk-11-slim AS builder
WORKDIR /app
COPY pom.xml .
# Download dependencies first to cache them
RUN mvn dependency:go-offline
COPY src/ ./src/
RUN mvn clean package

# Stage 2: Submit to Flink
FROM flink:1.18.0-scala_2.12
WORKDIR /opt/flink/bin
COPY --from=builder /app/target/feature-processor-1.0-SNAPSHOT.jar /opt/flink/job.jar

# Wait for JobManager to be ready, then submit the job
CMD ["sh", "-c", "sleep 30 && ./flink run -m flink-jobmanager:8081 -d /opt/flink/job.jar"]