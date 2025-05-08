<project xmlns="http://maven.apache.org/POM/4.0.0"
         xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance"
         xsi:schemaLocation="http://maven.apache.org/POM/4.0.0 http://maven.apache.org/xsd/maven-4.0.0.xsd">
    <modelVersion>4.0.0</modelVersion>

    <groupId>com.nedbank.kafka</groupId>
    <artifactId>azure-blob-uploader</artifactId>
    <version>1.0-SNAPSHOT</version>

    <dependencies>
        <!-- Azure Storage Blob SDK -->
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

        <!-- Compatible reactor-core -->
        <dependency>
            <groupId>io.projectreactor</groupId>
            <artifactId>reactor-core</artifactId>
            <version>3.4.13</version>
        </dependency>

        <!-- Compatible reactor-netty -->
        <dependency>
            <groupId>io.projectreactor.netty</groupId>
            <artifactId>reactor-netty</artifactId>
            <version>1.0.24</version>
        </dependency>

        <!-- Optional logging -->
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-api</artifactId>
            <version>1.7.36</version>
        </dependency>
        <dependency>
            <groupId>org.slf4j</groupId>
            <artifactId>slf4j-simple</artifactId>
            <version>1.7.36</version>
        </dependency>
    </dependencies>
</project>
