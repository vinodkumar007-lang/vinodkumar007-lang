public static SummaryPayload buildPayload(KafkaMessage message,
                                          List<SummaryProcessedFile> processedFiles,
                                          int pagesProcessed,
                                          String mobstatTriggerPath,
                                          int customersProcessed) {

    SummaryPayload payload = new SummaryPayload();

    payload.setBatchID(message.getBatchId());
    payload.setFileName(message.getBatchId() + ".csv");
    payload.setMobstatTriggerFile(mobstatTriggerPath);

    // Header block
    Header header = new Header();
    header.setTenantCode(message.getTenantCode());
    header.setChannelID(message.getChannelID());
    header.setAudienceID(message.getAudienceID());
    header.setSourceSystem(message.getSourceSystem());
    header.setProduct(message.getProduct());
    header.setJobName(message.getJobName());
    header.setTimestamp(Instant.now().toString());
    payload.setHeader(header);

    // Determine overall status
    String overallStatus = "Completed";
    if (processedFiles != null && !processedFiles.isEmpty()) {
        boolean allFailed = processedFiles.stream().allMatch(f -> "FAILURE".equalsIgnoreCase(f.getStatusCode()));
        boolean anyFailed = processedFiles.stream().anyMatch(f -> "FAILURE".equalsIgnoreCase(f.getStatusCode()) || "PARTIAL".equalsIgnoreCase(f.getStatusCode()));

        if (allFailed) overallStatus = "Failure";
        else if (anyFailed) overallStatus = "Partial";
    }

    // Metadata block
    Metadata metadata = new Metadata();
    metadata.setTotalFilesProcessed(customersProcessed);
    metadata.setProcessingStatus(overallStatus);
    metadata.setEventOutcomeCode("0");
    metadata.setEventOutcomeDescription("Success");
    payload.setMetadata(metadata);

    // Payload block
    Payload payloadDetails = new Payload();
    payloadDetails.setUniqueConsumerRef(message.getUniqueConsumerRef());
    payloadDetails.setUniqueECPBatchRef(message.getUniqueECPBatchRef());
    payloadDetails.setRunPriority(message.getRunPriority());
    payloadDetails.setEventID(message.getEventID());
    payloadDetails.setEventType(message.getEventType());
    payloadDetails.setRestartKey(message.getRestartKey());
    payloadDetails.setFileCount(pagesProcessed);
    payload.setPayload(payloadDetails);

    // ✅ Grouped Customer Summaries (24 customers → 38 records grouped)
    List<CustomerSummary> customerSummaries = buildCustomerSummaries(processedFiles);
    payload.setCustomerSummaries(customerSummaries);

    return payload;
}
