private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> errorMap,
        KafkaMessage msg
) throws IOException {

    List<String> folders = List.of("email", "archive", "mobstat", "print");
    Map<String, String> folderToOutputMethod = Map.of(
            "email", "EMAIL",
            "archive", "ARCHIVE",
            "mobstat", "MOBSTAT",
            "print", "PRINT"
    );

    Map<String, List<SummaryProcessedFile>> grouped = new LinkedHashMap<>();

    for (SummaryProcessedFile file : customerList) {
        String customerId = file.getCustomerId();
        String accountNumber = file.getAccountNumber();
        String outputMethod = file.getOutputMethod();

        String key = customerId + "::" + accountNumber + "::" + outputMethod;
        grouped.computeIfAbsent(key, k -> new ArrayList<>()).add(file);
    }

    List<SummaryProcessedFile> result = new ArrayList<>();

    for (Map.Entry<String, List<SummaryProcessedFile>> entry : grouped.entrySet()) {
        String[] parts = entry.getKey().split("::");
        String customerId = parts[0];
        String accountNumber = parts[1];
        String method = parts[2];

        if ("ARCHIVE".equalsIgnoreCase(method)) continue;

        SummaryProcessedFile main = entry.getValue().get(0);

        // Look for matching archive
        String archiveKey = customerId + "::" + accountNumber + "::ARCHIVE";
        SummaryProcessedFile archive = grouped.getOrDefault(archiveKey, List.of())
                                              .stream().findFirst().orElse(null);

        SummaryProcessedFile combined = new SummaryProcessedFile();
        combined.setCustomerId(customerId);
        combined.setAccountNumber(accountNumber);

        // Set EMAIL/MOBSTAT/PRINT details
        combined.setOutputMethod(main.getOutputMethod());
        combined.setBlobUrl(main.getBlobUrl());
        combined.setStatus(main.getStatus());

        // Set ARCHIVE fields
        combined.setArchiveOutputMethod("ARCHIVE");
        combined.setArchiveBlobUrl(archive != null ? archive.getBlobUrl() : null);
        combined.setArchiveStatus(archive != null ? archive.getStatus() : "FAILED");

        // Determine overallStatus
        boolean mainSuccess = "SUCCESS".equalsIgnoreCase(main.getStatus());
        boolean archiveSuccess = archive != null && "SUCCESS".equalsIgnoreCase(archive.getStatus());

        if (mainSuccess && archiveSuccess) {
            combined.setOverallStatus("SUCCESS");
        } else if (mainSuccess || archiveSuccess) {
            combined.setOverallStatus("PARTIAL");
        } else {
            combined.setOverallStatus("FAILED");
        }

        result.add(combined);
    }

    return result;
}

========

private SummaryPayload buildPayload(
        List<SummaryProcessedFile> combinedFiles,
        KafkaMessage msg,
        String fileName,
        String batchId
) {
    SummaryPayload payload = new SummaryPayload();
    payload.setBatchId(batchId);
    payload.setFileName(fileName);

    // Set header
    Header header = new Header();
    header.setTenantCode(msg.getTenantCode());
    header.setAudienceId(msg.getAudienceId());
    header.setChannelId(msg.getChannelId());
    header.setSourceSystem(msg.getSourceSystem());
    header.setConsumerReference(msg.getConsumerReference());
    header.setProcessReference(msg.getProcessReference());
    payload.setHeader(header);

    // Build metadata (excluding customer-level info)
    Metadata metadata = new Metadata();
    metadata.setTotalCustomers((int) combinedFiles.stream().map(SummaryProcessedFile::getCustomerId).distinct().count());
    metadata.setTotalRecords(combinedFiles.size());
    payload.setMetadata(metadata);

    // Group files by customer + account
    Map<String, List<SummaryProcessedFile>> customerMap = new LinkedHashMap<>();
    for (SummaryProcessedFile file : combinedFiles) {
        String key = file.getCustomerId() + "::" + file.getAccountNumber();
        customerMap.computeIfAbsent(key, k -> new ArrayList<>()).add(file);
    }

    // Add to customerSummaries
    List<CustomerSummary> summaries = new ArrayList<>();
    for (Map.Entry<String, List<SummaryProcessedFile>> entry : customerMap.entrySet()) {
        String[] parts = entry.getKey().split("::");
        CustomerSummary cs = new CustomerSummary();
        cs.setCustomerId(parts[0]);
        cs.setAccountNumber(parts[1]);
        cs.setProcessedFiles(entry.getValue());
        summaries.add(cs);
    }

    payload.setCustomerSummaries(summaries);

    return payload;
}
======
