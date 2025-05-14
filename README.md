private Map<String, Object> buildSummaryPayload(String batchId, String blobUrl, JsonNode batchFilesNode) {
    List<ProcessedFileInfo> processedFiles = new ArrayList<>();

    for (JsonNode fileNode : batchFilesNode) {
        String objectId = fileNode.get("ObjectId").asText();
        String fileLocation = fileNode.get("fileLocation").asText();
        String extension = getFileExtension(fileLocation);

        // Create a dynamic URL that includes both batchId and objectId
        String dynamicFileUrl = blobUrl + "/" + objectId.replaceAll("[{}]", "") + "_" + batchId + "_" + objectId + extension;

        processedFiles.add(new ProcessedFileInfo(objectId, dynamicFileUrl));
    }

    // Create the summary object
    SummaryPayload summary = new SummaryPayload();
    summary.setBatchID(batchId);
    summary.setHeader(new HeaderInfo()); // Populate header if required
    summary.setMetadata(new MetadataInfo()); // Populate metadata if required
    summary.setPayload(new PayloadInfo()); // Populate payload if required
    summary.setProcessedFiles(processedFiles);
    
    // Optionally, include a summary file URL
    summary.setSummaryFileURL(blobUrl + "/summary/" + batchId + "_summary.json");

    // Convert to Map and return
    ObjectMapper mapper = new ObjectMapper();
    return mapper.convertValue(summary, Map.class);
}
