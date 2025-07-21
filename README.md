private List<SummaryProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> errorMap
) {
    List<String> outputMethods = List.of("EMAIL", "MOBSTAT", "PRINT");

    Map<String, SummaryProcessedFileEntry> result = new LinkedHashMap<>();

    for (SummaryProcessedFile file : customerList) {
        String customerId = file.getCustomerId();
        String accountNumber = file.getAccountNumber();
        String outputMethod = file.getOutputMethod();

        boolean isArchive = "ARCHIVE".equalsIgnoreCase(outputMethod);
        if (!isArchive && !outputMethods.contains(outputMethod)) continue;

        String key = customerId + "::" + accountNumber + "::" + (isArchive ? file.getLinkedOutputMethod() : outputMethod);
        SummaryProcessedFileEntry entry = result.getOrDefault(key, new SummaryProcessedFileEntry());
        entry.setCustomerId(customerId);
        entry.setAccountNumber(accountNumber);
        entry.setOutputMethod(isArchive ? file.getLinkedOutputMethod() : outputMethod);

        if (isArchive) {
            entry.setArchiveUrl(file.getBlobUrl());
            entry.setArchiveStatus(file.getStatus());
        } else {
            entry.setOutputUrl(file.getBlobUrl());
            entry.setOutputStatus(file.getStatus());
        }

        result.put(key, entry);
    }

    // Set overallStatus
    for (SummaryProcessedFileEntry entry : result.values()) {
        String outputStatus = entry.getOutputStatus();
        String archiveStatus = entry.getArchiveStatus();

        if ("SUCCESS".equalsIgnoreCase(outputStatus) && "SUCCESS".equalsIgnoreCase(archiveStatus)) {
            entry.setOverallStatus("SUCCESS");
        } else if ("SUCCESS".equalsIgnoreCase(outputStatus) || "SUCCESS".equalsIgnoreCase(archiveStatus)) {
            entry.setOverallStatus("PARTIAL");
        } else {
            entry.setOverallStatus("FAILED");
        }
    }

    return new ArrayList<>(result.values());
}
=============

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

    List<SummaryProcessedFile> result = new ArrayList<>();

    for (SummaryProcessedFile file : customerList) {
        for (String folder : folders) {
            String key = file.getCustomerId() + "::" + file.getAccountNumber() + "::" + folder;
            String outputMethod = folderToOutputMethod.get(folder);
            SummaryProcessedFile copy = new SummaryProcessedFile();
            BeanUtils.copyProperties(file, copy);
            copy.setOutputMethod(outputMethod);

            Optional<Path> match = Files.walk(jobDir.resolve(folder))
                    .filter(p -> p.getFileName().toString().contains(file.getCustomerId())
                            && p.getFileName().toString().contains(file.getAccountNumber()))
                    .findFirst();

            if (match.isPresent()) {
                Path matchedPath = match.get();
                String blobUrl = blobStorageService.uploadFileAndReturnLocation(matchedPath, msg.getTenantCode(), folder, msg.getBatchID());
                copy.setBlobUrl(blobUrl);
                copy.setStatus("SUCCESS");

                if ("ARCHIVE".equalsIgnoreCase(outputMethod)) {
                    String linkedMethod = determineLinkedOutputMethod(jobDir, file, msg.getTenantCode(), msg.getBatchID());
                    copy.setLinkedOutputMethod(linkedMethod);
                }

            } else {
                copy.setStatus("FAILED");
                copy.setBlobUrl(null);
            }

            result.add(copy);
        }
    }

    return result;
}

private String determineLinkedOutputMethod(Path jobDir, SummaryProcessedFile file, String tenant, String batchID) {
    List<String> primaryFolders = List.of("email", "mobstat", "print");
    for (String folder : primaryFolders) {
        Path folderPath = jobDir.resolve(folder);
        try {
            Optional<Path> match = Files.walk(folderPath)
                    .filter(p -> p.getFileName().toString().contains(file.getCustomerId())
                            && p.getFileName().toString().contains(file.getAccountNumber()))
                    .findFirst();

            if (match.isPresent()) {
                return folder.toUpperCase(); // EMAIL, MOBSTAT, PRINT
            }
        } catch (IOException ignored) {}
    }
    return "UNKNOWN";
}
========

public static SummaryPayload buildPayload(
        KafkaMessage message,
        List<SummaryProcessedFile> customerList,
        int pagesProcessed,
        String printFiles,
        String mobstatTriggerUrl,
        int customersProcessed
) {
    SummaryPayload payload = new SummaryPayload();

    // Set Header
    SummaryHeader header = new SummaryHeader();
    header.setTenantCode(message.getTenantCode());
    header.setChannelID(message.getChannelID());
    header.setAudienceID(message.getAudienceID());
    header.setBatchID(message.getBatchID());
    header.setJobID(message.getJobID());
    header.setSourceSystem(message.getSourceSystem());
    header.setConsumerReference(message.getConsumerReference());
    header.setProcessReference(message.getProcessReference());
    header.setTimestamp(message.getTimestamp());
    payload.setHeader(header);

    // Set Meta
    SummaryMetadata meta = new SummaryMetadata();
    meta.setPagesProcessed(pagesProcessed);
    meta.setPrintFiles(printFiles);
    meta.setMobstatTriggerUrl(mobstatTriggerUrl);
    meta.setCustomersProcessed(customersProcessed);
    payload.setMetadata(meta);

    // ✅ Add Processed Files Grouped
    List<SummaryProcessedFileEntry> grouped = new SummaryJsonWriter().buildProcessedFileEntries(customerList, new HashMap<>());
    payload.setPayload(grouped);

    return payload;
}
