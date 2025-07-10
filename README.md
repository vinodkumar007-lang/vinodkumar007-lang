private void writeAndUploadMetadataJson(KafkaMessage message, Path jobDir) {
    try {
        Map<String, Object> metaMap = objectMapper.convertValue(message, Map.class);
        if (metaMap.containsKey("batchFiles")) {
            List<Map<String, Object>> files = (List<Map<String, Object>>) metaMap.get("batchFiles");
            for (Map<String, Object> f : files) {
                Object blob = f.remove("blobUrl");
                if (blob != null) {
                    f.put("mountPath", blob);  // ✅ Include mount path in metadata
                }
            }
        }

        String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(metaMap);

        // ✅ Log metadata.json before uploading
        logger.info("📝 Metadata JSON before sending to OT:\n{}", json);

        File metaFile = new File(jobDir.toFile(), "metadata.json");
        FileUtils.writeStringToFile(metaFile, json, StandardCharsets.UTF_8);
        String blobPath = String.format("%s/Trigger/metadata_%s.json", message.getSourceSystem(), message.getBatchId());
        blobStorageService.uploadFile(metaFile.getAbsolutePath(), blobPath);
        FileUtils.forceDelete(metaFile);
    } catch (Exception ex) {
        logger.error("metadata.json generation failed", ex);
    }
}
