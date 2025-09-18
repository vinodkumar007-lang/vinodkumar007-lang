private void sendToAuditTopic(AuditMessage auditMessage) {
    try {
        String auditJson = objectMapper.writeValueAsString(auditMessage);

        // Use async send to prevent blocking / timeout
        ListenableFuture<SendResult<String, String>> future =
                auditKafkaTemplate.send(auditTopic, auditMessage.getBatchId(), auditJson);

        future.addCallback(
                success -> logger.info("📣 Audit message sent for batchId {}: {}", auditMessage.getBatchId(), auditJson),
                failure -> logger.error("❌ Failed to send audit message for batchId {}: {}", auditMessage.getBatchId(), failure.getMessage(), failure)
        );

    } catch (JsonProcessingException e) {
        logger.error("❌ Failed to serialize audit message for batchId {}: {}", auditMessage.getBatchId(), e.getMessage(), e);
    }
}
