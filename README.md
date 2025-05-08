<?xml version="1.0" encoding="UTF-8"?>
<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.org.filemanager</groupId>
    <artifactId>file-manager</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <maven.compiler.source>17</maven.compiler.source>
        <maven.compiler.target>17</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>

    <repositories>

        <repository>
            <id>nexus-releases</id>
            <!--<name>maven-group</name>-->
            <url>https://nexus.devops.nednet.co.za/repository/maven-group</url>
        <snapshots>
        <enabled>false</enabled>
        </snapshots>
        </repository>

    </repositories>

    <dependencyManagement>
        <dependencies>
            <dependency>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-dependencies</artifactId>
                <type>pom</type>
                <version>3.0.0</version>
                <scope>import</scope>
            </dependency>
            <dependency>
                <groupId>org.slf4j</groupId>
                <artifactId>slf4j-parent</artifactId>
                <version>2.0.9</version> <!-- latest -->
                <type>pom</type>
                <scope>import</scope>
            </dependency>
            <dependency>
                <groupId>io.projectreactor</groupId>
                <artifactId>reactor-core</artifactId>
                <version>3.4.13</version>  <!-- Ensure the latest version -->
            </dependency>
            <dependency>
                <groupId>io.projectreactor.netty</groupId>
                <artifactId>reactor-netty</artifactId>
                <version>1.0.24</version>
            </dependency>
        </dependencies>
    </dependencyManagement>

    <dependencies>
        <!-- Kafka Dependencies for Spring Boot 3.x -->
        <dependency>
            <groupId>za.co.nedbank</groupId>
            <artifactId>spring-nedbank-kafka-sb3</artifactId>
            <version>1.0.0-RELEASE</version>
        </dependency>

        <!-- Kafka client dependency -->
        <dependency>
            <groupId>org.springframework.kafka</groupId>
            <artifactId>spring-kafka</artifactId>
            <version>3.0.11</version>
        </dependency>

        <!-- Spring Web for REST API -->
        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-web</artifactId>
        </dependency>

        <dependency>
            <groupId>org.springframework.boot</groupId>
            <artifactId>spring-boot-starter-integration</artifactId>
        </dependency>

        <!-- Spring Batch for Batch Processing -->
        <dependency>
            <groupId>org.springframework.batch</groupId>
            <artifactId>spring-batch-core</artifactId>
        </dependency>

        <!-- Spring for SOAP Web Service (for IBM FileNet integration) -->
        <dependency>
            <groupId>org.springframework.ws</groupId>
            <artifactId>spring-ws-core</artifactId>
        </dependency>

        <!-- Azure Blob Storage -->
        <dependency>
            <groupId>com.azure</groupId>
            <artifactId>azure-storage-blob</artifactId>
            <version>12.10.0</version>
            <exclusions>
                <exclusion>
                    <groupId>io.projectreactor.netty</groupId>
                    <artifactId>reactor-netty</artifactId>
                </exclusion>
                <exclusion>
                    <groupId>io.projectreactor</groupId>
                    <artifactId>reactor-core</artifactId>
                </exclusion>
            </exclusions>
        </dependency>
        <dependency>
            <groupId>com.azure</groupId>
            <artifactId>azure-core</artifactId>
            <version>1.14.0</version>  <!-- Update to the latest version -->
        </dependency>
        <!-- Azure Key Vault Secrets SDK -->
        <dependency>
            <groupId>com.azure</groupId>
            <artifactId>azure-security-keyvault-secrets</artifactId>
            <version>4.2.3</version>  <!-- Ensure the latest version -->
        </dependency>

        <!-- Azure Identity SDK (for credentials) -->
        <dependency>
            <groupId>com.azure</groupId>
            <artifactId>azure-identity</artifactId>
            <version>1.2.5</version>  <!-- Ensure the latest version -->
        </dependency>

        <!-- Reactor Core Dependency -->
        <dependency>
            <groupId>io.projectreactor</groupId>
            <artifactId>reactor-core</artifactId>
            <!-- Ensure the latest version -->
        </dependency>
        <!-- Compatible reactor-netty -->
        <dependency>
            <groupId>io.projectreactor.netty</groupId>
            <artifactId>reactor-netty</artifactId>
        </dependency>
        <!-- JSON processing library (Jackson or Gson) -->
        <dependency>
            <groupId>com.fasterxml.jackson.core</groupId>
            <artifactId>jackson-databind</artifactId>
            <!--<version>2.12.5</version>-->
        </dependency>
    </dependencies>
    <build>
        <plugins>
            <plugin>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-maven-plugin</artifactId>
                <version>3.0.0</version>
            </plugin>
        </plugins>
    </build>
</project>
