public ResponseDTO processMessages() {
    List<ProcessedFile> processedFiles = new ArrayList<>();
    List<PrintFile> printFiles = new ArrayList<>();
    SummaryPayload summaryPayload = new SummaryPayload();
    ObjectMapper objectMapper = new ObjectMapper();

    try {
        ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));
        for (ConsumerRecord<String, String> record : records) {
            JsonNode message = objectMapper.readTree(record.value());

            // Extract fields
            String sourceFileURL = message.has("sourceFileURL") ? message.get("sourceFileURL").asText() : null;
            String destinationPath = "processed/" + UUID.randomUUID() + ".pdf";

            // ✅ Upload file to Blob Storage
            String blobURL = storageService.copyFileFromUrlToBlob(sourceFileURL, destinationPath);

            // ✅ Add to processedFiles list
            ProcessedFile processedFile = new ProcessedFile();
            processedFile.setFileName(destinationPath);
            processedFile.setBlobURL(blobURL);
            processedFile.setStatus("Processed");
            processedFiles.add(processedFile);

            // Example: Add a print file if needed (customize if required)
            if (message.has("printFileURL")) {
                String printFileURL = message.get("printFileURL").asText();
                PrintFile printFile = new PrintFile();
                printFile.setPrintFileURL(printFileURL);
                printFiles.add(printFile);
            }

            // Optionally populate summaryPayload fields (header, metadata, etc.)
            if (message.has("batchID")) summaryPayload.setBatchID(message.get("batchID").asText());
            if (message.has("timestamp")) summaryPayload.setTimestamp(message.get("timestamp").asText());
            if (message.has("header")) summaryPayload.setHeader(objectMapper.treeToValue(message.get("header"), Header.class));
            if (message.has("payload")) summaryPayload.setPayload(objectMapper.treeToValue(message.get("payload"), Payload.class));
        }

        // ✅ Finalize summaryPayload
        summaryPayload.setProcessedFiles(processedFiles);
        summaryPayload.setPrintFiles(printFiles);

        // ✅ Upload summary.json
        String summaryFileURL = storageService.uploadSummaryJson(summaryPayload);
        summaryPayload.setSummaryFileURL(summaryFileURL);

        // ✅ Build final response
        ResponseDTO response = new ResponseDTO();
        response.setMessage("Success");
        response.setStatus("OK");
        response.setSummaryPayload(summaryPayload);
        return response;

    } catch (Exception e) {
        e.printStackTrace();
        ResponseDTO response = new ResponseDTO();
        response.setMessage("Error processing messages");
        response.setStatus("FAIL");
        return response;
    }
}
