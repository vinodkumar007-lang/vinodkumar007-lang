 private void sendToAuditTopic(AuditMessage auditMessage) {
        try {
            String auditJson = objectMapper.writeValueAsString(auditMessage);
            auditKafkaTemplate.send(auditTopic, auditMessage.getBatchId(), auditJson);
            logger.info("📣 Audit message sent for batchId {}: {}", auditMessage.getBatchId(), auditJson);
        } catch (JsonProcessingException e) {
            logger.error("❌ Failed to serialize audit message for batchId {}: {}", auditMessage.getBatchId(), e.getMessage(), e);
        }
    }
