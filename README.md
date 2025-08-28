    public static SummaryPayload buildPayload(
            KafkaMessage kafkaMessage,
            List<SummaryProcessedFile> processedList,
            String fileName,
            String batchId,
            String timestamp,
            Map<String, Map<String, String>> errorMap,
            List<PrintFile> printFiles
    ) {
        SummaryPayload payload = new SummaryPayload();
        payload.setBatchID(batchId);
        payload.setFileName(fileName);
        payload.setTimestamp(timestamp);

        // Populate header metadata
        Header header = new Header();
        header.setTenantCode(kafkaMessage.getTenantCode());
        header.setChannelID(kafkaMessage.getChannelID());
        header.setAudienceID(kafkaMessage.getAudienceID());
        header.setTimestamp(timestamp);
        header.setSourceSystem(kafkaMessage.getSourceSystem());
        header.setProduct(kafkaMessage.getSourceSystem());
        header.setJobName(kafkaMessage.getSourceSystem());
        payload.setHeader(header);

        // Build processed file entries from summary list
        List<ProcessedFileEntry> processedFileEntries = buildProcessedFileEntries(processedList, errorMap, printFiles);
        payload.setProcessedFileList(processedFileEntries);

        // Total unique file URLs (email, print, mobstat, archive)
        int totalUniqueFiles = (int) processedFileEntries.stream()
                .flatMap(entry -> Stream.of(
                        entry.getEmailBlobUrl(),
                        entry.getPrintBlobUrl(),
                        entry.getMobstatBlobUrl(),
                        entry.getArchiveBlobUrl()
                ))
                .filter(Objects::nonNull)
                .distinct()
                .count();

        // Populate payload details from KafkaMessage
        Payload payloadInfo = new Payload();
        payloadInfo.setUniqueECPBatchRef(kafkaMessage.getUniqueECPBatchRef());
        payloadInfo.setRunPriority(kafkaMessage.getRunPriority());
        payloadInfo.setEventID(kafkaMessage.getEventID());
        payloadInfo.setEventType(kafkaMessage.getEventType());
        payloadInfo.setRestartKey(kafkaMessage.getRestartKey());
        payloadInfo.setFileCount(totalUniqueFiles);
        payload.setPayload(payloadInfo);

        // Metadata: count distinct customers and determine final status
        Metadata metadata = new Metadata();

        // Count total customers based on archive entries only
        long totalArchiveEntries = processedFileEntries.stream()
                .filter(pf -> isNonEmpty(pf.getArchiveBlobUrl())).distinct()
                .count();
        metadata.setTotalCustomersProcessed((int) totalArchiveEntries);

        Set<String> statuses = processedFileEntries.stream()
                .map(ProcessedFileEntry::getOverallStatus)
                .collect(Collectors.toSet());

        String overallStatus;
        if (statuses.size() == 1) {
            overallStatus = statuses.iterator().next();
        } else if (statuses.contains("SUCCESS") && statuses.contains("FAILED")) {
            overallStatus = "PARTIAL";
        } else if (statuses.contains("PARTIAL") || statuses.size() > 1) {
            overallStatus = "PARTIAL";
        } else {
            overallStatus = "FAILED";
        }

        metadata.setProcessingStatus(overallStatus);
        metadata.setEventOutcomeCode("0");
        metadata.setEventOutcomeDescription(overallStatus.toLowerCase());
        payload.setMetadata(metadata);

        // ✅ Step 1: Assign status based on conditions before decoding
        for (PrintFile pf : printFiles) {
            String psUrl = pf.getPrintFileURL();

            if (psUrl != null && psUrl.endsWith(".ps")) {
                // .ps file exists, set SUCCESS
                pf.setPrintStatus("SUCCESS");
            } else if (psUrl != null && errorMap.containsKey(psUrl)) {
                // Error found for this file
                pf.setPrintStatus("FAILED");
            } else {
                // Not found or unclear
                pf.setPrintStatus("");
            }
        }

// ✅ Step 2: Decode and collect into final printFileList
        List<PrintFile> printFileList = new ArrayList<>();

        for (PrintFile pf : printFiles) {
            if (pf.getPrintFileURL() != null) {
                String decodedUrl = URLDecoder.decode(pf.getPrintFileURL(), StandardCharsets.UTF_8);

                PrintFile printFile = new PrintFile();
                printFile.setPrintFileURL(decodedUrl);
                printFile.setPrintStatus(pf.getPrintStatus() != null ? pf.getPrintStatus() : "");

                printFileList.add(printFile);
            }
        }

// ✅ Step 3: Set in payload
        payload.setPrintFiles(printFileList);

        return payload;
    }
