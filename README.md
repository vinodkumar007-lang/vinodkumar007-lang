// --- UPDATED METHOD: append each Kafka message summary to JSON ---
private File writeSummaryToFile(SummaryPayload summaryPayload) throws IOException {
    String userHome = System.getProperty("user.home");
    File jsonFile = new File(userHome, "summary.json");

    Map<String, Object> summaryData = new HashMap<>();
    summaryData.put("batchID", summaryPayload.getHeader().getBatchId());
    summaryData.put("fileName", summaryPayload.getHeader().getJobName());
    summaryData.put("header", summaryPayload.getHeader());
    summaryData.put("processedFiles", summaryPayload.getPayload().getProcessedFiles());
    summaryData.put("printFiles", summaryPayload.getPayload().getPrintFiles());

    // If file already exists, append to an array; otherwise start a new array
    List<Map<String, Object>> summaries;
    if (jsonFile.exists()) {
        try {
            summaries = objectMapper.readValue(jsonFile, List.class);
        } catch (Exception e) {
            summaries = new ArrayList<>();
        }
    } else {
        summaries = new ArrayList<>();
    }

    summaries.add(summaryData);
    objectMapper.writerWithDefaultPrettyPrinter().writeValue(jsonFile, summaries);

    summaryPayload.setSummaryFileURL(jsonFile.getAbsolutePath());
    summaryPayload.getMetadata().setSummaryFileURL(jsonFile.getAbsolutePath());

    return jsonFile;
}
