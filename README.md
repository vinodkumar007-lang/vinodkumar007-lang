public static void appendToSummaryJson(File summaryFile, SummaryPayload newPayload, String azureBlobStorageAccount) {
    try {
        ObjectNode root;
        if (summaryFile.exists()) {
            root = (ObjectNode) mapper.readTree(summaryFile);
        } else {
            // Create new root with basic structure using newPayload
            root = buildPayloadNode(newPayload, azureBlobStorageAccount);
        }

        // Build new payload node from incoming data
        ObjectNode newPayloadNode = buildPayloadNode(newPayload, azureBlobStorageAccount);

        // Merge existing JSON with new incoming payload to avoid duplicates
        root = mergeSummaryJson(root, newPayloadNode);

        // Write merged JSON back to file with pretty print
        mapper.writerWithDefaultPrettyPrinter().writeValue(summaryFile, root);
        logger.info("Appended to summary.json: {}", summaryFile.getAbsolutePath());

    } catch (IOException e) {
        logger.error("Error appending to summary.json", e);
    }
}
