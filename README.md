private void sendToAuditTopic(AuditMessage auditMessage) {
    try {
        String auditJson = objectMapper.writeValueAsString(auditMessage);

        // Send message asynchronously to audit topic
        ListenableFuture<SendResult<String, String>> future =
                auditKafkaTemplate.send(auditTopic, auditMessage.getBatchId(), auditJson);

        // Add success and failure callbacks
        future.addCallback(
                result -> logger.info("📣 Audit message sent successfully for batchId {}: {}", auditMessage.getBatchId(), auditJson),
                ex -> logger.error("❌ Failed to send audit message for batchId {}: {}", auditMessage.getBatchId(), ex.getMessage(), ex)
        );

    } catch (JsonProcessingException e) {
        logger.error("❌ Failed to serialize audit message for batchId {}: {}", auditMessage.getBatchId(), e.getMessage(), e);
    }
}
