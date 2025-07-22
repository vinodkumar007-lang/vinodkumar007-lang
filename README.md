public static SummaryPayload buildPayload(
        KafkaMessage kafkaMessage,
        List<SummaryProcessedFile> processedList,
        String summaryBlobUrl,
        String fileName,
        String batchId,
        String timestamp,
        Map<String, Map<String, String>> errorMap
) {
    SummaryPayload payload = new SummaryPayload();
    payload.setBatchID(batchId);
    payload.setFileName(fileName);
    payload.setTimestamp(timestamp);
    payload.setSummaryFileURL(summaryBlobUrl);

    Header header = new Header();
    header.setTenantCode(kafkaMessage.getTenantCode());
    header.setChannelID(kafkaMessage.getChannelID());
    header.setAudienceID(kafkaMessage.getAudienceID());
    header.setTimestamp(timestamp);
    header.setSourceSystem(kafkaMessage.getSourceSystem());
    header.setProduct(kafkaMessage.getSourceSystem());
    header.setJobName(kafkaMessage.getSourceSystem());
    payload.setHeader(header);

    List<ProcessedFileEntry> processedFileEntries = buildProcessedFileEntries(processedList, errorMap);
    payload.setProcessedFileList(processedFileEntries);

    int totalFileUrls = processedFileEntries.size();

    Payload payloadInfo = new Payload();
    payloadInfo.setUniqueECPBatchRef(kafkaMessage.getUniqueECPBatchRef());
    payloadInfo.setRunPriority(kafkaMessage.getRunPriority());
    payloadInfo.setEventID(kafkaMessage.getEventID());
    payloadInfo.setEventType(kafkaMessage.getEventType());
    payloadInfo.setRestartKey(kafkaMessage.getRestartKey());
    payloadInfo.setFileCount(totalFileUrls);
    payload.setPayload(payloadInfo);

    Metadata metadata = new Metadata();
    metadata.setTotalCustomersProcessed((int) processedFileEntries.stream()
            .map(pf -> pf.getCustomerId() + "::" + pf.getAccountNumber())
            .distinct()
            .count());

    long total = processedFileEntries.size();
    long success = processedFileEntries.stream()
            .filter(entry -> "SUCCESS".equalsIgnoreCase(entry.getOverallStatus()))
            .count();
    long failed = processedFileEntries.stream()
            .filter(entry -> "FAILED".equalsIgnoreCase(entry.getOverallStatus()))
            .count();

    String overallStatus;
    if (success == total) {
        overallStatus = "SUCCESS";
    } else if (failed == total) {
        overallStatus = "FAILED";
    } else {
        overallStatus = "PARTIAL";
    }

    metadata.setProcessingStatus(overallStatus);
    metadata.setEventOutcomeCode("0");
    metadata.setEventOutcomeDescription(overallStatus.toLowerCase());
    payload.setMetadata(metadata);

    return payload;
}

=========
private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedList,
        Map<String, Map<String, String>> errorMap // key = customerId::accountNumber, value = Map<outputMethod, reason>
) {
    List<ProcessedFileEntry> finalList = new ArrayList<>();

    if (processedList == null || processedList.isEmpty()) {
        return finalList;
    }

    // Group by customerId::accountNumber
    Map<String, List<SummaryProcessedFile>> grouped = processedList.stream()
            .filter(f -> f.getCustomerId() != null && f.getAccountNumber() != null)
            .collect(Collectors.groupingBy(f -> f.getCustomerId() + "::" + f.getAccountNumber()));

    for (Map.Entry<String, List<SummaryProcessedFile>> entry : grouped.entrySet()) {
        String[] parts = entry.getKey().split("::");
        String customerId = parts[0];
        String accountNumber = parts[1];
        List<SummaryProcessedFile> records = entry.getValue();

        // Map of outputMethod -> SummaryProcessedFile
        Map<String, SummaryProcessedFile> typeMap = records.stream()
                .collect(Collectors.toMap(SummaryProcessedFile::getOutputMethod, f -> f, (a, b) -> a));

        SummaryProcessedFile archive = typeMap.get("ARCHIVE");
        if (archive == null) continue; // Archive is mandatory

        // Collect requested outputMethods from errorMap and typeMap keys
        Set<String> requestedOutputs = new HashSet<>(errorMap.getOrDefault(entry.getKey(), Collections.emptyMap()).keySet());
        requestedOutputs.addAll(typeMap.keySet());
        requestedOutputs.remove("ARCHIVE"); // Exclude archive itself

        for (String outputMethod : requestedOutputs) {
            ProcessedFileEntry entryObj = new ProcessedFileEntry();
            entryObj.setCustomerId(customerId);
            entryObj.setAccountNumber(accountNumber);
            entryObj.setOutputMethod(outputMethod);

            // Archive info
            entryObj.setArchiveBlobUrl(archive.getBlobURL());
            entryObj.setArchiveStatus(archive.getStatus());

            SummaryProcessedFile output = typeMap.get(outputMethod);
            if (output != null) {
                entryObj.setOutputBlobUrl(output.getBlobURL());
                entryObj.setOutputStatus(output.getStatus());
            } else {
                Map<String, String> failedMap = errorMap.getOrDefault(entry.getKey(), Collections.emptyMap());
                entryObj.setOutputBlobUrl(null);

                if (failedMap.containsKey(outputMethod)) {
                    entryObj.setOutputStatus("FAILED");
                } else {
                    entryObj.setOutputStatus("NOT_FOUND");
                }
            }

            // Compute overallStatus for this outputMethod + archive pair
            String archiveStatus = entryObj.getArchiveStatus();
            String outputStatus = entryObj.getOutputStatus();

            if ("SUCCESS".equalsIgnoreCase(outputStatus) && "SUCCESS".equalsIgnoreCase(archiveStatus)) {
                entryObj.setOverallStatus("SUCCESS");
            } else if ("FAILED".equalsIgnoreCase(outputStatus) || "FAILED".equalsIgnoreCase(archiveStatus)) {
                entryObj.setOverallStatus("FAILED");
            } else {
                entryObj.setOverallStatus("PARTIAL");
            }

            finalList.add(entryObj);
        }
    }

    return finalList;
}
