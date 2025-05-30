FROM docker.ncr.devops.nednet.co.za/nedbank/openjdk-17:latest
WORKDIR /app
COPY . /app
COPY target/file-manager-1.0-SNAPSHOT.jar .
COPY truststore.jks /usr/lib/jvm/java-17-openjdk/lib/security/truststore.jks
COPY keystore.jks /usr/lib/jvm/java-17-openjdk/lib/security/keystore.jks
EXPOSE 8080
CMD ["java", "-jar", "file-manager-1.0-SNAPSHOT.jar"]
