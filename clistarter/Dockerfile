FROM gradle:jdk21-corretto

# Set working directory in the container
WORKDIR /app

# Copy the JAR file into the container
COPY ./build/libs/clistarter.jar clistarter.jar

# Add metadata labels
LABEL org.opencontainers.image.source="https://github.com/fleaktech/zephflow-core"
LABEL org.opencontainers.image.description="ZephFlow CLI Starter - Java data processing framework"
LABEL org.opencontainers.image.licenses="Apache-2.0"

# Define the entrypoint that will pass arguments to the JAR
ENTRYPOINT ["java", "-jar", "clistarter.jar"]

# Set default command to show help
CMD ["-h"]