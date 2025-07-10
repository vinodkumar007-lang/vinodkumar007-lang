FROM openjdk:17-jdk-slim
WORKDIR /app
COPY target/file-manager-1.0-SNAPSHOT.jar .
EXPOSE 8080
CMD ["java", "-jar", "file-manager-1.0-SNAPSHOT.jar"]
