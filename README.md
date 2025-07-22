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

        List<ProcessedFileEntry> processedFileEntries = buildProcessedFileEntries(processedList);
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

    private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedFiles) {
        Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

        for (SummaryProcessedFile file : processedFiles) {
            String key = file.getCustomerId() + "-" + file.getAccountNumber();
            ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());

            entry.setCustomerId(file.getCustomerId());
            entry.setAccountNumber(file.getAccountNumber());

            switch (file.getOutputType()) {
                case "EMAIL":
                    entry.setPdfEmailFileUrl(file.getBlobURL());
                    entry.setPdfEmailFileUrlStatus(file.getStatus());
                    break;
                case "ARCHIVE":
                    entry.setArchiveBlobUrl(file.getBlobURL());
                    entry.setArchiveStatus(file.getStatus());
                    break;
                case "MOBSTAT":
                    entry.setPdfMobstatFileUrl(file.getBlobURL());
                    entry.setPdfMobstatFileUrlStatus(file.getStatus());
                    break;
                // PRINT is intentionally excluded
            }

            grouped.put(key, entry);
        }

        for (ProcessedFileEntry entry : grouped.values()) {
            String emailStatus = entry.getPdfEmailFileUrlStatus();
            String archiveStatus = entry.getArchiveStatus();
            String mobstatStatus = entry.getPdfMobstatFileUrlStatus();

            boolean emailPresent = emailStatus != null;
            boolean archivePresent = archiveStatus != null;

            boolean emailSuccess = "SUCCESS".equalsIgnoreCase(emailStatus);
            boolean archiveSuccess = "SUCCESS".equalsIgnoreCase(archiveStatus);
            boolean mobstatSuccess = mobstatStatus == null || "SUCCESS".equalsIgnoreCase(mobstatStatus);

            // Determine overall status logic
            if (emailSuccess && archiveSuccess && mobstatSuccess) {
                entry.setOverallStatus("SUCCESS");
            } else if (emailPresent && "FAILED".equalsIgnoreCase(emailStatus)) {
                entry.setOverallStatus("FAILED");
            } else if (!emailPresent && archiveSuccess) {
                entry.setOverallStatus("SUCCESS");
            } else if (!emailPresent && !archiveSuccess) {
                entry.setOverallStatus("FAILED");
            } else {
                entry.setOverallStatus("PARTIAL");
            }
        }

        return new ArrayList<>(grouped.values());
    }
