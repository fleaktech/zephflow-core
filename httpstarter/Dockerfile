FROM gradle:jdk21-corretto

WORKDIR /app

COPY ./build/libs/httpstarter.jar app.jar

LABEL org.opencontainers.image.source="https://github.com/fleaktech/zephflow-core"
LABEL org.opencontainers.image.description="ZephFlow HTTP Starter - Java data processing framework"
LABEL org.opencontainers.image.licenses="Apache-2.0"

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]