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
    private static List<ProcessedFileEntry> buildProcessedFileEntries(
            List<SummaryProcessedFile> processedFiles,
            Map<String, Map<String, String>> errorMap) {

        Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

        for (SummaryProcessedFile file : processedFiles) {
            String key = file.getCustomerId() + "-" + file.getAccountNumber();
            ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());

            entry.setCustomerId(file.getCustomerId());
            entry.setAccountNumber(file.getAccountNumber());

            String outputType = file.getOutputType() != null ? file.getOutputType().toUpperCase(Locale.ROOT) : "";
            String blobUrl = file.getBlobUrl();
            String status = file.getStatus();

            String errorKey = file.getCustomerId() + "-" + file.getAccountNumber();
            boolean isErrorPresent = errorMap.containsKey(errorKey);

            if (blobUrl == null || blobUrl.trim().isEmpty()) {
                if (isErrorPresent && !outputType.equals("ARCHIVE")) {
                    status = "FAILED";
                } else {
                    status = "";
                }
            }

            switch (outputType) {
                case "EMAIL":
                    entry.setEmailBlobUrl(blobUrl);
                    entry.setEmailStatus(status);
                    break;
                case "ARCHIVE":
                    entry.setArchiveBlobUrl(blobUrl);
                    entry.setArchiveStatus(status);
                    break;
                case "PRINT":
                    entry.setPrintBlobUrl(blobUrl);
                    entry.setPrintStatus(status);
                    break;
                case "MOBSTAT":
                    entry.setMobstatBlobUrl(blobUrl);
                    entry.setMobstatStatus(status);
                    break;
            }

            grouped.put(key, entry);
        }

        for (String errorKey : errorMap.keySet()) {
            if (!grouped.containsKey(errorKey)) {
                String[] parts = errorKey.split("-");
                if (parts.length == 2) {
                    ProcessedFileEntry errorEntry = new ProcessedFileEntry();
                    errorEntry.setCustomerId(parts[0]);
                    errorEntry.setAccountNumber(parts[1]);
                    errorEntry.setEmailStatus("FAILED");
                    errorEntry.setOverallStatus("FAILED");
                    grouped.put(errorKey, errorEntry);
                }
            }
        }

        // ✅ Enhanced: Fill missing output channels with FAILED if errorMap says so
        for (ProcessedFileEntry entry : grouped.values()) {
            String errorKey = entry.getCustomerId() + "-" + entry.getAccountNumber();
            boolean isErrorPresent = errorMap.containsKey(errorKey);

            if (isErrorPresent) {
                Map<String, String> channelErrors = errorMap.get(errorKey);

                if ((entry.getEmailStatus() == null || entry.getEmailStatus().isEmpty())
                        && channelErrors.containsKey("EMAIL")) {
                    entry.setEmailStatus("FAILED");
                }

                if ((entry.getPrintStatus() == null || entry.getPrintStatus().isEmpty())
                        && channelErrors.containsKey("PRINT")) {
                    entry.setPrintStatus("FAILED");
                }

                if ((entry.getMobstatStatus() == null || entry.getMobstatStatus().isEmpty())
                        && channelErrors.containsKey("MOBSTAT")) {
                    entry.setMobstatStatus("FAILED");
                }
            }

            List<String> statuses = new ArrayList<>();
            if (entry.getEmailStatus() != null && !entry.getEmailStatus().isEmpty()) statuses.add(entry.getEmailStatus());
            if (entry.getMobstatStatus() != null && !entry.getMobstatStatus().isEmpty()) statuses.add(entry.getMobstatStatus());
            if (entry.getPrintStatus() != null && !entry.getPrintStatus().isEmpty()) statuses.add(entry.getPrintStatus());
            if (entry.getArchiveStatus() != null && !entry.getArchiveStatus().isEmpty()) statuses.add(entry.getArchiveStatus());

            boolean allSuccess = !statuses.isEmpty() && statuses.stream().allMatch(s -> "SUCCESS".equalsIgnoreCase(s));
            boolean anyFailed = statuses.stream().anyMatch(s -> "FAILED".equalsIgnoreCase(s));
            boolean allFailed = !statuses.isEmpty() && statuses.stream().allMatch(s -> "FAILED".equalsIgnoreCase(s));

            String overallStatus;
            if (isErrorPresent) {
                overallStatus = "FAILED";
            } else if (allSuccess) {
                overallStatus = "SUCCESS";
            } else if (allFailed) {
                overallStatus = "FAILED";
            } else if (anyFailed) {
                overallStatus = "PARTIAL";
            } else if ("SUCCESS".equalsIgnoreCase(entry.getArchiveStatus()) && statuses.size() == 1) {
                overallStatus = "SUCCESS";
            } else {
                overallStatus = "FAILED";
            }

            // 🆕 Additional safeguard: Archive is SUCCESS, but other delivery types failed or missing
            if ("SUCCESS".equalsIgnoreCase(entry.getArchiveStatus()) && isErrorPresent) {
                Map<String, String> channelErrors = errorMap.get(errorKey);

                boolean emailMissing = (entry.getEmailBlobUrl() == null || entry.getEmailBlobUrl().trim().isEmpty())
                        && channelErrors.containsKey("EMAIL");
                boolean printMissing = (entry.getPrintBlobUrl() == null || entry.getPrintBlobUrl().trim().isEmpty())
                        && channelErrors.containsKey("PRINT");
                boolean mobstatMissing = (entry.getMobstatBlobUrl() == null || entry.getMobstatBlobUrl().trim().isEmpty())
                        && channelErrors.containsKey("MOBSTAT");

                if (emailMissing || printMissing || mobstatMissing) {
                    overallStatus = "FAILED";
                }
            }

            entry.setOverallStatus(overallStatus);
        }

        long fileCount = grouped.values().stream()
                .flatMap(entry -> Stream.of(
                        new AbstractMap.SimpleEntry<>(entry.getEmailBlobUrl(), entry.getEmailStatus()),
                        new AbstractMap.SimpleEntry<>(entry.getPrintBlobUrl(), entry.getPrintStatus()),
                        new AbstractMap.SimpleEntry<>(entry.getMobstatBlobUrl(), entry.getMobstatStatus()),
                        new AbstractMap.SimpleEntry<>(entry.getArchiveBlobUrl(), entry.getArchiveStatus())
                ))
                .filter(e -> e.getKey() != null && !e.getKey().trim().isEmpty()
                        && "SUCCESS".equalsIgnoreCase(e.getValue()))
                .count();

        System.out.println("✅ Final fileCount (blobUrls with SUCCESS): " + fileCount);
        return new ArrayList<>(grouped.values());
    }
