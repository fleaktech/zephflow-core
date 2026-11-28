# Corretto 21, pinned by digest so Renovate proposes base updates. GraalVM is only used as
# Maven libraries (Polyglot, GraalPy), which run on any JDK 21.
FROM public.ecr.aws/amazoncorretto/amazoncorretto:27-al2023@sha256:6a7c05b122ae52ff3b281570e03432a3a14b0161f67a44ff69c37e7da67f130f

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