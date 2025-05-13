<dependency>
  <groupId>com.azure</groupId>
  <artifactId>azure-core-http-netty</artifactId>
  <version>1.6.3</version>
  <exclusions>
    <exclusion>
      <groupId>io.projectreactor.netty</groupId>
      <artifactId>reactor-netty</artifactId>
    </exclusion>
  </exclusions>
</dependency>
<dependency>
  <groupId>com.azure</groupId>
  <artifactId>azure-core-http-okhttp</artifactId>
  <version>1.11.5</version>
</dependency>

BlobServiceClientBuilder builder = new BlobServiceClientBuilder()
    .endpoint("<your-endpoint>")
    .credential(new StorageSharedKeyCredential(accountName, accountKey))
    .httpClient(new OkHttpAsyncHttpClientBuilder().build());
