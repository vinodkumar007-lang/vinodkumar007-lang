private SummaryPayload buildPayload(List<SummaryProcessedFile> processedList, KafkaMessage message) {
    SummaryPayload payload = new SummaryPayload();

    // ⛳ Header (from Kafka message)
    SummaryHeader header = new SummaryHeader();
    header.setTenantCode(message.getTenantCode());
    header.setBatchID(message.getBatchId());
    header.setChannelID(message.getChannelId());
    header.setAudienceID(message.getAudienceId());
    header.setSourceSystem(message.getSourceSystem());
    header.setConsumerReference(message.getConsumerReference());
    header.setProcessReference(message.getProcessReference());
    header.setMessageID(message.getMessageId());
    payload.setHeader(header);

    // 📁 FileName from Kafka
    payload.setFileName(message.getFileName());

    // ⏱ Timestamp
    payload.setTimestamp(LocalDateTime.now().toString());

    // ✅ Group processed files by customerId + accountNumber
    List<SummaryProcessedFileGroup> groups = new ArrayList<>();
    Map<String, SummaryProcessedFileGroup> groupMap = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedList) {
        if (file.getCustomerId() == null || file.getAccountNumber() == null) continue;

        String key = file.getCustomerId() + "::" + file.getAccountNumber();
        SummaryProcessedFileGroup group = groupMap.computeIfAbsent(key, k -> {
            SummaryProcessedFileGroup g = new SummaryProcessedFileGroup();
            g.setCustomerId(file.getCustomerId());
            g.setAccountNumber(file.getAccountNumber());
            return g;
        });

        String url = file.getBlobURL();
        if (url == null) continue;

        String decodedUrl = URLDecoder.decode(url, StandardCharsets.UTF_8);
        String status = file.getStatus();

        if (decodedUrl.contains("/email/")) {
            group.setPdfEmailFileUrl(decodedUrl);
            group.setPdfEmailFileUrlStatus(status);
        } else if (decodedUrl.contains("/archive/")) {
            group.setPdfArchiveFileUrl(decodedUrl);
            group.setPdfArchiveFileUrlStatus(status);
        } else if (decodedUrl.contains("/mobstat/")) {
            group.setPdfMobstatFileUrl(decodedUrl);
            group.setPdfMobstatFileUrlStatus(status);
        } else if (decodedUrl.contains("/print/")) {
            group.setPrintFileUrl(decodedUrl);
            group.setPrintFileUrlStatus(status);
        }

        // Set overall status once
        if (group.getOverallStatus() == null) {
            group.setOverallStatus(file.getOverallStatus());
        }
    }

    groups.addAll(groupMap.values());

    // 🔁 Add to payload
    payload.setProcessedFiles(groups);

    return payload;
}
