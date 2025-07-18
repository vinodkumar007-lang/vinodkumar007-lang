public static SummaryPayload buildPayload(
        String batchId,
        String fileName,
        KafkaMessage message,
        List<SummaryProcessedFile> allProcessedFiles,
        String triggerFileUrl,
        String tenantCode,
        String channelId,
        String audienceId,
        String timestamp
) {
    SummaryPayload payload = new SummaryPayload();
    payload.setBatchID(batchId);
    payload.setFileName(fileName);

    SummaryHeader header = new SummaryHeader();
    header.setSourceSystem(message.getSourceSystem());
    header.setProcessReference(message.getProcessReference());
    header.setConsumerReference(message.getConsumerReference());
    header.setTimestamp(timestamp);

    SummaryMetadata metadata = new SummaryMetadata();
    metadata.setTenantCode(tenantCode);
    metadata.setChannelID(channelId);
    metadata.setAudienceID(audienceId);

    // Group by customer
    Map<String, List<SummaryProcessedFile>> groupedByCustomer = allProcessedFiles.stream()
            .collect(Collectors.groupingBy(SummaryProcessedFile::getCustomerNumber));

    List<CustomerSummary> customerSummaries = new ArrayList<>();

    for (Map.Entry<String, List<SummaryProcessedFile>> entry : groupedByCustomer.entrySet()) {
        String customer = entry.getKey();
        List<SummaryProcessedFile> records = entry.getValue();

        String overallStatus;
        boolean hasFailed = records.stream().anyMatch(r -> "FAILED".equalsIgnoreCase(r.getStatus()));
        boolean hasSuccess = records.stream().anyMatch(r -> "SUCCESS".equalsIgnoreCase(r.getStatus()));

        if (hasFailed) {
            overallStatus = "PARTIAL";
        } else if (hasSuccess) {
            overallStatus = "SUCCESS";
        } else {
            overallStatus = "NOT_FOUND";
        }

        CustomerSummary customerSummary = new CustomerSummary();
        customerSummary.setCustomerNumber(customer);
        customerSummary.setProcessedFiles(records);
        customerSummary.setOverallStatus(overallStatus);

        customerSummaries.add(customerSummary);
    }

    metadata.setCustomerSummaries(customerSummaries);
    payload.setHeader(header);
    payload.setMetadata(metadata);

    // Add processedList at root if needed (flattened list for backward compatibility)
    payload.setProcessedList(allProcessedFiles);

    // Add trigger file URL if exists
    if (triggerFileUrl != null) {
        payload.setTriggerFileUrl(triggerFileUrl);
    }

    return payload;
}
