String connectionString = String.format(
                    "DefaultEndpointsProtocol=https;AccountName=%s;AccountKey=%s;EndpointSuffix=core.windows.net",
                    accountName, accountKey
            );

            /*BlobContainerClient containerClient = new BlobContainerClientBuilder()
                    .connectionString(connectionString)
                    .containerName(containerName)
                    .buildClient();*/

            BlobServiceClientBuilder builder = new BlobServiceClientBuilder()
                    .endpoint(connectionString)
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .httpClient(new OkHttpAsyncHttpClientBuilder().build());

            BlobClient blobClient = builder.getBlobClient(blobName);
