// ✅ Include all structured payloads + summary.json file path
Map<String, Object> result = new HashMap<>();
result.put("status", "success");
result.put("message", "Batch processed successfully");
result.put("summaryFileUrl", summaryFilePath); // local file system path to summary.json
result.put("summaryPayload", summaryPayload);  // all enriched info in structured form

return result;
