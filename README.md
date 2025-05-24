# Use an official OpenJDK runtime as a parent image
FROM openjdk:17-jdk-alpine

# Set working directory inside the container
WORKDIR /app

# Copy the JAR file into the container
COPY app.jar .

# Expose port if your app runs on a port (optional)
EXPOSE 8080

# Command to run the JAR file
CMD ["java", "-jar", "app.jar"]
