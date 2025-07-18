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
        metadata.setEventOutcomeDescription(overallStatus.toLowerCase());
        payload.setMetadata(metadata);

        return payload;
    }


=====================

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
        metadata.setEventOutcomeDescription(overallStatus.toLowerCase());
        payload.setMetadata(metadata);

        return payload;
    }

    private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedList) {
        Map<String, ProcessedFileEntry> entryMap = new LinkedHashMap<>();
        Map<String, List<String>> statusTracker = new HashMap<>();

        for (SummaryProcessedFile file : processedList) {
            String customerId = file.getCustomerId();
            String accountNumber = file.getAccountNumber();
            String blobURL = file.getBlobURL();
            String status = file.getStatus() != null ? file.getStatus() : "UNKNOWN";

            // Skip if both URL and status are null or status is NOT_FOUND
            if ((blobURL == null && !"FAILED".equalsIgnoreCase(status)) ||
                    customerId == null || accountNumber == null) {
                continue;
            }

            String key = customerId + "::" + accountNumber;

            ProcessedFileEntry entry = entryMap.computeIfAbsent(key, k -> {
                ProcessedFileEntry e = new ProcessedFileEntry();
                e.setCustomerId(customerId);
                e.setAccountNumber(accountNumber);
                return e;
            });

            // Track status for overallStatus computation
            statusTracker.computeIfAbsent(key, k -> new ArrayList<>()).add(status);

            String lowerUrl = blobURL != null ? URLDecoder.decode(blobURL, StandardCharsets.UTF_8).toLowerCase() : "";

            if (lowerUrl.contains("/email/")) {
                entry.setPdfEmailFileUrl(blobURL);
                entry.setPdfEmailFileUrlStatus(status);
            } else if (lowerUrl.contains("/archive/")) {
                entry.setPdfArchiveFileUrl(blobURL);
                entry.setPdfArchiveFileUrlStatus(status);
            } else if (lowerUrl.contains("/mobstat/")) {
                entry.setPdfMobstatFileUrl(blobURL);
                entry.setPdfMobstatFileUrlStatus(status);
            } else if (lowerUrl.contains("/print/")) {
                entry.setPrintFileUrl(blobURL);
                entry.setPrintFileUrlStatus(status);
            } else {
                // If URL is null but status is FAILED (e.g., generation failure), track placeholder
                if ("FAILED".equalsIgnoreCase(status)) {
                    entry.setPdfArchiveFileUrl(null); // or skip setting fileUrl
                    entry.setPdfArchiveFileUrlStatus("FAILED");
                }
            }
        }

        // Set overallStatus per customer-account group
        for (Map.Entry<String, ProcessedFileEntry> groupedEntry : entryMap.entrySet()) {
            List<String> statuses = statusTracker.getOrDefault(groupedEntry.getKey(), List.of());

            String overallStatus;
            if (statuses.stream().allMatch(s -> "SUCCESS".equalsIgnoreCase(s))) {
                overallStatus = "SUCCESS";
            } else if (statuses.stream().anyMatch(s -> "FAILED".equalsIgnoreCase(s))) {
                overallStatus = "PARTIAL";
            } else {
                overallStatus = "UNKNOWN";
            }

            groupedEntry.getValue().setOverAllStatusCode(overallStatus);
        }

        return new ArrayList<>(entryMap.values());
    }
