for (BatchFile file : message.getBatchFiles()) {
    try {
        String sourceBlobUrl = file.getBlobUrl();
        String inputFileName = file.getFilename();
        if (inputFileName != null && !inputFileName.isBlank()) {
            fileName = inputFileName;
        }

        // ✅ Handle full URL or blob name safely
        String resolvedBlobPath = extractBlobPath(sourceBlobUrl);

        // ✅ Now safe to pass to blob service
        String inputFileContent = blobStorageService.downloadFileContent(resolvedBlobPath);

        List<CustomerData> customers = DataParser.extractCustomerData(inputFileContent);


        private String extractBlobPath(String blobPathOrUrl) {
    if (blobPathOrUrl.startsWith("https://")) {
        try {
            URI uri = new URI(blobPathOrUrl);
            String path = uri.getPath(); // /container/blobname
            // Strip leading slash and container name
            int idx = path.indexOf('/', 1); // skip initial /
            return (idx > 0) ? path.substring(idx + 1) : path.substring(1);
        } catch (Exception e) {
            logger.warn("⚠️ Failed to extract blob path from URL: {}", blobPathOrUrl, e);
            return blobPathOrUrl; // Fallback to original
        }
    }
    return blobPathOrUrl; // Already a path
}
