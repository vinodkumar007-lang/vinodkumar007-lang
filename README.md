 String sourceSystem = message.getSourceSystem();
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
