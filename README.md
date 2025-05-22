String userHome = System.getProperty("user.home");
File jsonFile = new File(userHome, "summary.json");

Map<String, Object> summaryJson = new LinkedHashMap<>();
summaryJson.put("batchID", batchId);
summaryJson.put("fileName", fileName);

// Create header map
Map<String, Object> headerMap = new LinkedHashMap<>();
headerMap.put("tenantCode", extractField(root, "tenantCode"));
headerMap.put("channelID", extractField(root, "channelId"));
headerMap.put("audienceID", extractField(root, "audienceId"));
headerMap.put("timestamp", new Date().toInstant().toString());
headerMap.put("sourceSystem", extractField(root, "sourceSystem"));
headerMap.put("product", extractField(root, "product"));
headerMap.put("jobName", jobName);

summaryJson.put("header", headerMap);
summaryJson.put("processedFiles", processedFiles);
summaryJson.put("printFiles", printFiles);

try {
    objectMapper.writerWithDefaultPrettyPrinter().writeValue(jsonFile, summaryJson);
} catch (IOException e) {
    return generateErrorResponse("601", "Failed to write summary file");
}
