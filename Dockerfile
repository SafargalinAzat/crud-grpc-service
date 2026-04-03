FROM maven:3.9-eclipse-temurin-17 AS builder

WORKDIR /app

COPY pom.xml .
RUN mvn dependency:go-offline -B
COPY src ./src
RUN mvn clean package -DskipTests


FROM eclipse-temurin:17-jre-alpine
WORKDIR /app
COPY --from=builder /app/target/crud-grpc-service-1.0.0.jar /app/app.jar


ENTRYPOINT ["java", "-jar", "/app/app.jar"]