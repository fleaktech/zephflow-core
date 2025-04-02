FROM gradle:jdk21-corretto AS builder

WORKDIR /app
COPY . /app/
RUN ./gradlew :httpstarter:build

FROM eclipse-temurin:21-jre-alpine

WORKDIR /app

# Create a non-root user to run the application
RUN addgroup -S appgroup && adduser -S appuser -G appgroup

# Copy JAR from builder stage
COPY --from=builder /app/httpstarter/build/libs/httpstarter.jar app.jar

# Set ownership for the application files
RUN chown -R appuser:appgroup /app

# Labels
LABEL org.opencontainers.image.source="https://github.com/fleaktech/zephflow-core"
LABEL org.opencontainers.image.description="ZephFlow HTTP Starter - Java data processing framework"
LABEL org.opencontainers.image.licenses="Apache-2.0"

EXPOSE 8080

# Switch to non-root user
USER appuser

ENTRYPOINT ["java", "-jar", "app.jar"]