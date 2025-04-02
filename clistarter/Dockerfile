FROM gradle:jdk21-corretto

# Set working directory in the container
WORKDIR /app

# Copy the JAR file into the container
COPY ./build/libs/clistarter.jar clistarter.jar

# Define the entrypoint that will pass arguments to the JAR
ENTRYPOINT ["java", "-jar", "clistarter.jar"]

# Set default command to show help
CMD ["-h"]