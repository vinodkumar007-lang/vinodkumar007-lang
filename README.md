private SummaryPayload buildPayload(
        String batchId,
        String fileName,
        KafkaMessage kafkaMessage,
        List<SummaryProcessedFile> processedList,
        List<PrintFileEntry> printFiles
) {
    SummaryPayload payload = new SummaryPayload();

    // ⛳ 1. Set header
    SummaryHeader header = new SummaryHeader();
    header.setTenantCode(kafkaMessage.getTenantCode());
    header.setChannelId(kafkaMessage.getChannelId());
    header.setAudienceId(kafkaMessage.getAudienceId());
    header.setBatchId(batchId);
    header.setFileName(fileName);
    payload.setHeader(header);

    // 🧾 2. Group per customer + account and map URLs/statuses
    Map<String, ProcessedFileEntry> outputMap = new LinkedHashMap<>();

    for (SummaryProcessedFile spf : processedList) {
        String customer = spf.getCustomerId();
        String account = spf.getAccountNumber();
        String key = customer + "::" + account;

        ProcessedFileEntry entry = outputMap.computeIfAbsent(key, k -> {
            ProcessedFileEntry e = new ProcessedFileEntry();
            e.setCustomerId(customer);
            e.setAccountNumber(account);
            return e;
        });

        String url = spf.getBlobFileURL();
        if (url == null) continue;

        if (url.contains("/archive/")) {
            entry.setPdfArchiveFileUrl(url);
            entry.setPdfArchiveFileUrlStatus(spf.getStatus());
        } else if (url.contains("/email/")) {
            entry.setPdfEmailFileUrl(url);
            entry.setPdfEmailFileUrlStatus(spf.getStatus());
        } else if (url.contains("/mobstat/")) {
            entry.setPdfMobstatFileUrl(url);
            entry.setPdfMobstatFileUrlStatus(spf.getStatus());
        } else if (url.contains("/print/")) {
            entry.setPrintFileUrl(url);
            entry.setPrintFileUrlStatus(spf.getStatus());
        }

        // Optional: Update overall status (can be improved to compute best overall)
        entry.setStatusCode(spf.getStatus());
        entry.setStatusDescription("SUCCESS".equalsIgnoreCase(spf.getStatus()) ? "Processed Successfully" : "Failed");
    }

    // ✅ 3. Set payload (customer-level entries)
    payload.setPayload(new ArrayList<>(outputMap.values()));

    // ✅ 4. Set printFiles block
    payload.setPrintFiles(printFiles != null ? printFiles : Collections.emptyList());

    return payload;
}
