public static void buildPayload(
        SummaryPayload payload,
        List<SummaryProcessedFile> processedFiles,
        KafkaMessage message,
        String fileName,
        String summaryBlobUrl
) {
    payload.setBatchID(message.getBatchID());
    payload.setFileName(fileName);
    payload.setSummaryFileURL(summaryBlobUrl);

    // Set header
    SummaryHeader header = new SummaryHeader();
    header.setTenantCode(message.getTenantCode());
    header.setChannelID(message.getChannelID());
    header.setAudienceID(message.getAudienceID());
    header.setCampaignID(message.getCampaignID());
    header.setProcessReference(message.getProcessReference());
    header.setSourceSystem(message.getSourceSystem());
    header.setTimestamp(Instant.now().toString());
    payload.setHeader(header);

    // Group by customerId
    Map<String, List<SummaryProcessedFile>> grouped = processedFiles.stream()
            .filter(spf -> !"TRIGGER".equalsIgnoreCase(spf.getFileType())) // skip trigger
            .collect(Collectors.groupingBy(SummaryProcessedFile::getCustomerId));

    List<SummaryProcessedRecord> summaryProcessedRecords = new ArrayList<>();

    for (Map.Entry<String, List<SummaryProcessedFile>> entry : grouped.entrySet()) {
        String customerId = entry.getKey();
        List<SummaryProcessedFile> customerFiles = entry.getValue();

        SummaryProcessedRecord record = new SummaryProcessedRecord();
        record.setCustomerId(customerId);
        record.setAccountNumber(customerFiles.get(0).getAccountNumber());

        List<ProcessedFileGroup> processedFileGroups = new ArrayList<>();

        // Group by output type (email/archive/print/mobstat)
        Map<String, List<SummaryProcessedFile>> typeMap = customerFiles.stream()
                .collect(Collectors.groupingBy(spf -> {
                    String url = Optional.ofNullable(spf.getBlobUrl()).orElse("").toLowerCase();
                    if (url.contains("email")) return "EMAIL";
                    else if (url.contains("archive")) return "ARCHIVE";
                    else if (url.contains("print")) return "PRINT";
                    else if (url.contains("mobstat")) return "MOBSTAT";
                    else return "UNKNOWN";
                }));

        for (Map.Entry<String, List<SummaryProcessedFile>> typeEntry : typeMap.entrySet()) {
            String type = typeEntry.getKey();
            List<SummaryProcessedFile> filesByType = typeEntry.getValue();

            ProcessedFileGroup group = new ProcessedFileGroup();
            group.setType(type);
            group.setFiles(filesByType);

            // Determine group status
            boolean allSuccess = filesByType.stream().allMatch(f -> "SUCCESS".equalsIgnoreCase(f.getStatus()));
            boolean anyFailed = filesByType.stream().anyMatch(f -> "FAILED".equalsIgnoreCase(f.getStatus()));

            String status = allSuccess ? "SUCCESS" : (anyFailed ? "FAILED" : "NOT_FOUND");
            group.setStatus(status);

            processedFileGroups.add(group);
        }

        // Set overall status for customer
        boolean anyFailed = customerFiles.stream().anyMatch(f -> "FAILED".equalsIgnoreCase(f.getStatus()));
        boolean anySuccess = customerFiles.stream().anyMatch(f -> "SUCCESS".equalsIgnoreCase(f.getStatus()));
        String overallStatus = anyFailed ? "PARTIAL" : (anySuccess ? "SUCCESS" : "NOT_FOUND");

        record.setOverallStatus(overallStatus);
        record.setProcessedFileGroups(processedFileGroups);
        summaryProcessedRecords.add(record);
    }

    payload.setProcessedList(summaryProcessedRecords);
}

public class SummaryProcessedRecord {
    private String customerId;
    private String accountNumber;
    private String overallStatus;
    private List<ProcessedFileGroup> processedFileGroups;
    // Getters and setters
}

public class ProcessedFileGroup {
    private String type; // email/archive/mobstat/print
    private String status; // SUCCESS / FAILED / NOT_FOUND
    private List<SummaryProcessedFile> files;
    // Getters and setters
}
