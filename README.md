FROM docker.ncr.devops.nednet.co.za/nedbank/openjdk-17:latest
WORKDIR /app
COPY target/file-manager-1.0-SNAPSHOT.jar .
COPY file-manager/truststore.jsk /app/truststore.jsk
EXPOSE 8080
CMD ["java", "-jar", "file-manager-1.0-SNAPSHOT.jar"]
 
Step 4/17 : COPY file-manager/truststore.jsk /app/truststore.jsk
COPY failed: file not found in build context or excluded by .dockerignore: stat file-manager/truststore.jsk: file does not exist
 
##[error]DEPRECATED: The legacy builder is deprecated and will be removed in a future release.
##[error]            Install the buildx component to build images with BuildKit:
##[error]            https://docs.docker.com/go/buildx/
##[error]COPY failed: file not found in build context or excluded by .dockerignore: stat file-manager/truststore.jsk: file does not exist
##[error]The process '/usr/bin/docker' failed with exit code 1


Docker Build Overview
Learn about Docker Build and its components.
 
