public static SummaryPayload buildPayload(KafkaMessage message,
                                          List<SummaryProcessedFile> processedFiles,
                                          int pagesProcessed,
                                          List<PrintFile> printFiles,
                                          String mobstatTriggerPath,
                                          int customersProcessed) {

    SummaryPayload payload = new SummaryPayload();

    payload.setBatchID(message.getBatchId());
    payload.setFileName(message.getBatchId() + ".csv");
    payload.setMobstatTriggerFile(mobstatTriggerPath);

    Header header = new Header();
    header.setTenantCode(message.getTenantCode());
    header.setChannelID(message.getChannelID());
    header.setAudienceID(message.getAudienceID());
    header.setSourceSystem(message.getSourceSystem());
    header.setProduct(message.getProduct());
    header.setJobName(message.getJobName());
    header.setTimestamp(Instant.now().toString());
    payload.setHeader(header);

    String overallStatus = "Completed";
    if (processedFiles != null && !processedFiles.isEmpty()) {
        boolean allFailed = processedFiles.stream().allMatch(f -> "FAILURE".equalsIgnoreCase(f.getStatusCode()));
        boolean anyFailed = processedFiles.stream().anyMatch(f ->
                "FAILURE".equalsIgnoreCase(f.getStatusCode()) || "PARTIAL".equalsIgnoreCase(f.getStatusCode()));
        if (allFailed) overallStatus = "Failure";
        else if (anyFailed) overallStatus = "Partial";
    }

    Metadata metadata = new Metadata();
    metadata.setTotalFilesProcessed(customersProcessed);
    metadata.setProcessingStatus(overallStatus);
    metadata.setEventOutcomeCode("0");
    metadata.setEventOutcomeDescription("Success");
    payload.setMetadata(metadata);

    Payload payloadDetails = new Payload();
    payloadDetails.setUniqueConsumerRef(message.getUniqueConsumerRef());
    payloadDetails.setUniqueECPBatchRef(message.getUniqueECPBatchRef());
    payloadDetails.setRunPriority(message.getRunPriority());
    payloadDetails.setEventID(message.getEventID());
    payloadDetails.setEventType(message.getEventType());
    payloadDetails.setRestartKey(message.getRestartKey());
    payloadDetails.setFileCount(pagesProcessed);
    payload.setPayload(payloadDetails);

    payload.setProcessedFiles(processedFiles); // keep raw list (optional)

    // ✅ Group and set customer summaries
    List<CustomerSummary> customerSummaries = groupProcessedFilesByCustomerId(processedFiles);
    payload.setCustomerSummaries(customerSummaries);

    // Optional: include printFiles
    payload.setPrintFiles(printFiles);

    return payload;
}

private static List<CustomerSummary> groupProcessedFilesByCustomerId(List<SummaryProcessedFile> processedFiles) {
    Map<String, List<SummaryProcessedFile>> customerGroupedMap = new HashMap<>();

    for (SummaryProcessedFile file : processedFiles) {
        if (file.getCustomerId() == null) continue;

        customerGroupedMap
                .computeIfAbsent(file.getCustomerId(), k -> new ArrayList<>())
                .add(file);
    }

    List<CustomerSummary> result = new ArrayList<>();
    for (Map.Entry<String, List<SummaryProcessedFile>> entry : customerGroupedMap.entrySet()) {
        String customerId = entry.getKey();
        List<SummaryProcessedFile> customerFiles = entry.getValue();

        CustomerSummary cs = new CustomerSummary();
        cs.setCustomerId(customerId);

        // Optional: total counts
        cs.setTotalAccounts((int) customerFiles.stream().map(SummaryProcessedFile::getAccountNumber).distinct().count());
        cs.setTotalSuccess((int) customerFiles.stream().filter(f -> "SUCCESS".equalsIgnoreCase(f.getStatus())).count());
        cs.setTotalFailure((int) customerFiles.stream().filter(f -> !"SUCCESS".equalsIgnoreCase(f.getStatus())).count());

        // Group by account for AccountSummary
        Map<String, List<SummaryProcessedFile>> accountGrouped = new HashMap<>();
        for (SummaryProcessedFile file : customerFiles) {
            if (file.getAccountNumber() == null) continue;

            accountGrouped
                    .computeIfAbsent(file.getAccountNumber(), k -> new ArrayList<>())
                    .add(file);
        }

        for (Map.Entry<String, List<SummaryProcessedFile>> accEntry : accountGrouped.entrySet()) {
            AccountSummary acc = new AccountSummary();
            acc.setAccountNumber(accEntry.getKey());

            // You can customize if needed – below is generic blob/status setting
            for (SummaryProcessedFile file : accEntry.getValue()) {
                String blobUrl = file.getBlobURL();
                String status = file.getStatus();

                // fallback if you want to set a generic field (optional)
                acc.setPrintBlobUrl(blobUrl); 
                acc.setPrintStatus(status);
            }

            cs.getAccounts().add(acc);
        }

        result.add(cs);
    }

    return result;
}
