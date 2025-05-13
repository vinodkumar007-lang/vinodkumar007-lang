OkHttpClient client = new OkHttpClient.Builder()
    .proxy(new Proxy(Proxy.Type.HTTP, new InetSocketAddress("your.proxy.host", 8080)))  // Set proxy
    .proxyAuthenticator(new Authenticator() {
        @Override
        public Request authenticate(Route route, Response response) throws IOException {
            String credential = Credentials.basic("your.proxy.username", "your.proxy.password");
            return response.request().newBuilder().header("Proxy-Authorization", credential).build();
        }
    })
    .build();

// Now use this client in your Azure Blob Storage client:
BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
    .endpoint("<your-azure-blob-endpoint>")
    .credential(new StorageSharedKeyCredential(accountName, accountKey))
    .httpClient(client)  // Set custom HTTP client with proxy
    .buildClient();
