FROM openjdk:17-jdk-slim

WORKDIR /app

COPY target/file-manager-app.jar .

EXPOSE 8080

CMD ["java", "-jar", "file-manager-app.jar"]
