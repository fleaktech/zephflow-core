# Corretto 21, pinned by digest so Renovate proposes base updates. GraalVM is only used as
# Maven libraries (Polyglot, GraalPy), which run on any JDK 21.
FROM public.ecr.aws/amazoncorretto/amazoncorretto:21-al2023@sha256:4d861b84f93fc8dfb4943686234f485c8d708bf36d88a45a61175d2663b66f38

WORKDIR /app

# Create a non-root user to run the application (the al2023 base has no groupadd/useradd)
RUN dnf install -y shadow-utils && dnf clean all \
    && groupadd -r appgroup && useradd -r -g appgroup appuser

# Copy JAR (built externally by GitHub Actions)
COPY ./build/libs/*-all.jar clistarter.jar

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