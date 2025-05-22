private Map<String, Object> handleMessage(String message) throws JsonProcessingException {
    // ✅ Log the incoming Kafka message
    logger.info("Received Kafka message: {}", message);

    JsonNode root;
    try {
        root = objectMapper.readTree(message);
    } catch (Exception e) {
        message = convertPojoToJson(message);
        try {
            root = objectMapper.readTree(message);
        } catch (Exception retryEx) {
            logger.error("Failed to parse corrected JSON", retryEx);
            return generateErrorResponse("400", "Invalid JSON format");
        }
    }

    // ... rest of your logic continues unchanged
