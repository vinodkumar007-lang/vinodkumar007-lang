<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.org.filemanager</groupId>
    <artifactId>file-manager</artifactId>
    <version>1.0-SNAPSHOT</version>

    <properties>
        <java.version>17</java.version>
        <maven.compiler.source>17</maven.compiler.source>
        <maven.compiler.target>17</maven.compiler.target>
        <project.build.sourceEncoding>UTF-8</project.build.sourceEncoding>
    </properties>

    <repositories>
        <repository>
            <id>nexus-releases</id>
            <url>https://nexus.devops.nednet.co.za/repository/maven-group</url>
            <snapshots>
                <enabled>false</enabled>
            </snapshots>
        </repository>
    </repositories>

    <dependencyManagement>
        <dependencies>
            <!-- Spring Boot BOM -->
            <dependency>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-dependencies</artifactId>
                <version>3.0.0</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
            <!-- SLF4J BOM -->
            <dependency>
                <groupId>org.slf4j</groupId>
                <artifactId>slf4j-parent</artifactId>
                <version>2.0.9</version>
                <type>pom</type>
                <scope>import</scope>
            </dependency>
            <dependency>
                <groupId>io.projectreactor.netty</groupId>
                <artifactId>reactor-netty</artifactId>
                <version>1.1.11</version>
            </dependency>
        </dependencies>
    </dependencyManagement>
        <dependencies>
            <!-- Azure SDK -->
            <dependency>
                <groupId>com.azure</groupId>
                <artifactId>azure-storage-blob</artifactId>
                <version>12.10.0</version>
                <exclusions>
                    <exclusion>
                        <groupId>io.projectreactor.netty</groupId>
                        <artifactId>reactor-netty</artifactId>
                    </exclusion>
                </exclusions>
            </dependency>

            <dependency>
                <groupId>com.azure</groupId>
                <artifactId>azure-core</artifactId>
                <version>1.14.0</version>
            </dependency>

            <dependency>
                <groupId>com.azure</groupId>
                <artifactId>azure-core-http-okhttp</artifactId>
                <version>1.4.0</version>
            </dependency>

            <!-- Optional, only if used -->
            <dependency>
                <groupId>com.azure</groupId>
                <artifactId>azure-security-keyvault-secrets</artifactId>
                <version>4.2.3</version>
            </dependency>

            <dependency>
                <groupId>com.azure</groupId>
                <artifactId>azure-identity</artifactId>
                <version>1.2.5</version>
            </dependency>

            <!-- Spring Boot and Kafka -->
            <dependency>
                <groupId>org.springframework.boot</groupId>
                <artifactId>spring-boot-starter-web</artifactId>
            </dependency>

            <dependency>
                <groupId>org.springframework.kafka</groupId>
                <artifactId>spring-kafka</artifactId>
                <version>3.0.11</version>
            </dependency>

            <!-- Vault integration -->
            <dependency>
                <groupId>org.json</groupId>
                <artifactId>json</artifactId>
                <version>20210307</version>
            </dependency>

            <!-- Jackson -->
            <dependency>
                <groupId>com.fasterxml.jackson.core</groupId>
                <artifactId>jackson-databind</artifactId>
            </dependency>

            <!-- Lombok -->
            <dependency>
                <groupId>org.projectlombok</groupId>
                <artifactId>lombok</artifactId>
                <scope>provided</scope>
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
