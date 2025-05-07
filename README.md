FROM openjdk:17-jdk-slim
WORKDIR /app
COPY target/file-manager.jar app.jar
ENTRYPOINT ["java", "-jar", "app.jar"]
EXPOSE 8080
