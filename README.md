String sourceSystem = message.getSourceSystem();

// Sanity check URLs before usage
if ("DEBTMAN".equalsIgnoreCase(sourceSystem) && (otOrchestrationApiUrl == null || otOrchestrationApiUrl.isBlank())) {
    logger.error("❌ otOrchestrationApiUrl is not configured for source system 'DEBTMAN' in batch {}", batchId);
    throw new IllegalArgumentException("otOrchestrationApiUrl is not configured");
}

if ("MFC".equalsIgnoreCase(sourceSystem) && (orchestrationMfcUrl == null || orchestrationMfcUrl.isBlank())) {
    logger.error("❌ orchestrationMfcUrl is not configured for source system 'MFC' in batch {}", batchId);
    throw new IllegalArgumentException("orchestrationMfcUrl is not configured");
}

String url = switch (sourceSystem.toUpperCase()) {
    case "DEBTMAN" -> otOrchestrationApiUrl;
    case "MFC" -> orchestrationMfcUrl;
    default -> {
        logger.error("❌ Unsupported source system '{}' in batch {}", sourceSystem, batchId);
        throw new IllegalArgumentException("Unsupported source system: " + sourceSystem);
    }
};


             // ✅ Sanitize path components to prevent directory traversal attacks
            String sanitizedBatchId = batchId.replaceAll("[^a-zA-Z0-9_-]", "_");
            String sanitizedSourceSystem = message.getSourceSystem().replaceAll("[^a-zA-Z0-9_-]", "_");

            "BatchId" : "54bf11dc-a912-490b-b02f-7a0aa228ee06",
                                                    "SourceSystem" : "DEBTMAN",
