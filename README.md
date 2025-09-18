/**
 * Updated method using CompletableFuture for Spring Kafka 3.x+
 */
private void sendToAuditTopic(AuditMessage auditMessage) {
    try {
        String auditJson = objectMapper.writeValueAsString(auditMessage);

        CompletableFuture<SendResult<String, String>> future =
                auditKafkaTemplate.send(auditTopic, auditMessage.getBatchId(), auditJson);

        future.whenComplete((result, ex) -> {
            if (ex != null) {
                logger.error("❌ Failed to send audit message for batchId {}: {}", auditMessage.getBatchId(), ex.getMessage(), ex);
            } else {
                logger.info("📣 Audit message sent successfully for batchId {}: {}", auditMessage.getBatchId(), auditJson);
            }
        });

    } catch (JsonProcessingException e) {
        logger.error("❌ Failed to serialize audit message for batchId {}: {}", auditMessage.getBatchId(), e.getMessage(), e);
    }
}
