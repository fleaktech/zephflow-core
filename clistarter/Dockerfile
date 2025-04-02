FROM gradle:jdk21-corretto

WORKDIR /app

# Create a non-root user to run the application
RUN groupadd -r appgroup && useradd -r -g appgroup appuser

# Copy JAR (built externally by GitHub Actions)
COPY ./build/libs/clistarter.jar clistarter.jar

# Set ownership for the application files
RUN chown -R appuser:appgroup /app

LABEL org.opencontainers.image.source="https://github.com/fleaktech/zephflow-core"
LABEL org.opencontainers.image.description="ZephFlow CLI Starter - Java data processing framework"
LABEL org.opencontainers.image.licenses="Apache-2.0"

# Switch to non-root user
USER appuser

ENTRYPOINT ["java", "-jar", "clistarter.jar"]

# Set default command to show help
CMD ["-h"]