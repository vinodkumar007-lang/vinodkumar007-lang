[<dependency>
  <groupId>com.fasterxml.jackson.datatype</groupId>
  <artifactId>jackson-datatype-jsr310</artifactId>
  <version>2.14.1</version> <!-- Match your jackson-databind version -->
</dependency>

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.fasterxml.jackson.databind.SerializationFeature;
private final ObjectMapper objectMapper;
this.objectMapper = new ObjectMapper();
        this.objectMapper.registerModule(new JavaTimeModule());
        this.objectMapper.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);


](https://nsndvextr01.blob.core.windows.net/nsndevextrm01/CASHMAN/57c8aaa1-105d-41a0-8864-faeaaed89ba7/19ef9d68-b119-4806-b09b-95a6c5fa4644/summary_57c8aaa1-105d-41a0-8864-faeaaed89ba7.json")
