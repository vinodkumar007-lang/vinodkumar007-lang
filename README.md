public static SummaryPayload buildPayload(
        KafkaMessage kafkaMessage,
        List<SummaryProcessedFile> processedList,
        String summaryBlobUrl,
        String fileName,
        String batchId,
        String timestamp
) {
    SummaryPayload payload = new SummaryPayload();
    payload.setBatchID(batchId);
    payload.setFileName(fileName);
    payload.setTimestamp(timestamp);
    payload.setSummaryFileURL(summaryBlobUrl);

    // HEADER
    Header header = new Header();
    header.setTenantCode(kafkaMessage.getTenantCode());
    header.setChannelID(kafkaMessage.getChannelID());
    header.setAudienceID(kafkaMessage.getAudienceID());
    header.setTimestamp(timestamp);
    header.setSourceSystem(kafkaMessage.getSourceSystem());
    header.setProduct(kafkaMessage.getSourceSystem());
    header.setJobName(kafkaMessage.getSourceSystem());
    payload.setHeader(header);

    // ✅ Final Processed Entries
    List<ProcessedFileEntry> processedFileEntries = buildProcessedFileEntries(processedList);
    payload.setProcessedFileList(processedFileEntries);

    // ✅ Dynamically count total non-null URLs (files actually added to summary)
    int totalFileUrls = processedFileEntries.stream()
            .mapToInt(entry -> {
                int count = 0;
                if (entry.getPdfEmailFileUrl() != null && !entry.getPdfEmailFileUrl().isBlank()) count++;
                if (entry.getPdfArchiveFileUrl() != null && !entry.getPdfArchiveFileUrl().isBlank()) count++;
                if (entry.getPdfMobstatFileUrl() != null && !entry.getPdfMobstatFileUrl().isBlank()) count++;
                if (entry.getPrintFileUrl() != null && !entry.getPrintFileUrl().isBlank()) count++;
                return count;
            })
            .sum();

    // PAYLOAD BLOCK
    Payload payloadInfo = new Payload();
    payloadInfo.setUniqueECPBatchRef(kafkaMessage.getUniqueECPBatchRef());
    payloadInfo.setRunPriority(kafkaMessage.getRunPriority());
    payloadInfo.setEventID(kafkaMessage.getEventID());
    payloadInfo.setEventType(kafkaMessage.getEventType());
    payloadInfo.setRestartKey(kafkaMessage.getRestartKey());
    payloadInfo.setFileCount(totalFileUrls);
    payload.setPayload(payloadInfo);

    // METADATA
    Metadata metadata = new Metadata();
    metadata.setTotalCustomersProcessed((int) processedFileEntries.stream()
            .map(pf -> pf.getCustomerId() + "::" + pf.getAccountNumber())
            .distinct()
            .count());

    // ✅ Determine overall status (Success / Partial / Failure)
    long total = processedFileEntries.size();
    long success = processedFileEntries.stream()
            .filter(entry -> "SUCCESS".equalsIgnoreCase(entry.getOverAllStatusCode()))
            .count();
    long failure = processedFileEntries.stream()
            .filter(entry -> "FAILURE".equalsIgnoreCase(entry.getOverAllStatusCode()))
            .count();

    String overallStatus;
    if (success == total) {
        overallStatus = "SUCCESS";
    } else if (failure == total) {
        overallStatus = "FAILURE";
    } else {
        overallStatus = "PARTIAL";
    }

    metadata.setProcessingStatus(overallStatus);
    metadata.setEventOutcomeCode("0");
    metadata.setEventOutcomeDescription("Summary generation " + overallStatus.toLowerCase());
    payload.setMetadata(metadata);

    return payload;
}

private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> errorMap,
        KafkaMessage msg) throws IOException {

    List<String> folders = List.of("email", "archive", "mobstat", "print");
    Map<String, String> folderToOutputMethod = Map.of(
            "email", "EMAIL",
            "archive", "ARCHIVE",
            "mobstat", "MOBSTAT",
            "print", "PRINT"
    );

    Map<String, List<SummaryProcessedFile>> groupedMap = new LinkedHashMap<>();

    // Track which folders are actually present
    Set<String> availableFolders = folders.stream()
            .filter(folder -> Files.exists(jobDir.resolve(folder)))
            .collect(Collectors.toSet());

    for (String folder : availableFolders) {
        Path folderPath = jobDir.resolve(folder);

        List<Path> files = Files.list(folderPath)
                .filter(Files::isRegularFile)
                .collect(Collectors.toList());

        for (Path file : files) {
            String fileName = file.getFileName().toString();

            for (SummaryProcessedFile customer : customerList) {
                String customerId = customer.getCustomerId();
                String accountNumber = customer.getAccountNumber();

                if (fileName.contains(customerId) && fileName.contains(accountNumber)) {
                    String key = customerId + "::" + accountNumber;

                    SummaryProcessedFile entry = buildCopy(customer);
                    entry.setFileName(fileName);
                    entry.setOutputMethod(folderToOutputMethod.get(folder));
                    entry.setStatus("SUCCESS");
                    entry.setStatusDescription("Processed successfully");
                    entry.setBlobURL("https://" + msg.getTargetContainer() + "/" + msg.getTargetDirectory() + "/" + folder + "/" + fileName);

                    groupedMap.computeIfAbsent(key, k -> new ArrayList<>()).add(entry);
                    break;
                }
            }
        }
    }

    // Add FAILED for missing files in available folders only
    for (SummaryProcessedFile customer : customerList) {
        String customerId = customer.getCustomerId();
        String accountNumber = customer.getAccountNumber();
        String key = customerId + "::" + accountNumber;

        List<SummaryProcessedFile> files = groupedMap.getOrDefault(key, new ArrayList<>());
        Set<String> existingMethods = files.stream()
                .map(SummaryProcessedFile::getOutputMethod)
                .collect(Collectors.toSet());

        for (String folder : availableFolders) {
            String method = folderToOutputMethod.get(folder);

            if (!existingMethods.contains(method)) {
                SummaryProcessedFile entry = buildCopy(customer);
                entry.setOutputMethod(method);
                entry.setBlobURL(null);
                entry.setStatus("FAILED");
                entry.setStatusDescription("File not found for method: " + method);

                // Use errorMap if matches
                if (errorMap.containsKey(customerId)) {
                    Map<String, String> error = errorMap.get(customerId);
                    if (accountNumber.equals(error.get("account")) && method.equalsIgnoreCase(error.get("method"))) {
                        entry.setStatusDescription("Marked as failed from ErrorReport");
                        entry.setBlobURL(error.get("errorBlobUrl"));
                    }
                }

                files.add(entry);
            }
        }

        groupedMap.put(key, files);
    }

    // Flatten
    return groupedMap.values().stream()
            .flatMap(List::stream)
            .collect(Collectors.toList());
}
