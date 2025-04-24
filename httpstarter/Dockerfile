FROM ghcr.io/graalvm/graalvm-community:21

WORKDIR /app

# Create a non-root user to run the application
RUN groupadd -r appgroup && useradd -r -g appgroup appuser

# Copy JAR (built externally by GitHub Actions)
COPY ./build/libs/httpstarter.jar app.jar

# Set ownership for the application files
RUN chown -R appuser:appgroup /app

LABEL org.opencontainers.image.source="https://github.com/fleaktech/zephflow-core"
LABEL org.opencontainers.image.description="ZephFlow HTTP Starter - Java data processing framework"
LABEL org.opencontainers.image.licenses="Apache-2.0"

EXPOSE 8080

# Switch to non-root user
USER appuser

ENTRYPOINT ["java", "-jar", "app.jar"]