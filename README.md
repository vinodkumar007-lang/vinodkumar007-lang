public void uploadDummyFile(String blobName) {
    try {
        // Create a BlobServiceClient
        BlobServiceClient blobServiceClient = new BlobServiceClientBuilder()
                .connectionString(CONNECTION_STRING)
                .buildClient();

        // Get or create container
        BlobContainerClient containerClient = blobServiceClient.getBlobContainerClient(CONTAINER_NAME);
        if (!containerClient.exists()) {
            containerClient.create();
        }

        // Prepare dummy file content
        String dummyContent = "This is a dummy file.\nCreated on: " + java.time.LocalDateTime.now();
        byte[] contentBytes = dummyContent.getBytes(StandardCharsets.UTF_8);
        InputStream dataStream = new ByteArrayInputStream(contentBytes);

        // Upload as blob
        BlobClient blobClient = containerClient.getBlobClient(blobName);
        blobClient.upload(dataStream, contentBytes.length, true); // true = overwrite if exists

        // ✅ Print the Blob URL
        System.out.println("✅ Dummy file uploaded to Azure Blob Storage!");
        System.out.println("📁 Blob Name: " + blobName);
        System.out.println("🌐 Blob URL : " + blobClient.getBlobUrl());

    } catch (Exception e) {
        e.printStackTrace();
        throw new RuntimeException("Failed to upload dummy file", e);
    }
}
