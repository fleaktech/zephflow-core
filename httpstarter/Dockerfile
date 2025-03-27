FROM gradle:jdk21-corretto

WORKDIR /app

COPY ./build/libs/httpstarter.jar app.jar

EXPOSE 8080

ENTRYPOINT ["java", "-jar", "app.jar"]