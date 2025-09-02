1.Can this function be split into manageable chunks?
2.Is this not duplication for the above for loop, can they be merged?
public static SummaryPayload buildPayload(
            KafkaMessage kafkaMessage,
            List<SummaryProcessedFile> processedList,
            String fileName,
            String batchId,
            String timestamp,
            Map<String, Map<String, String>> errorMap,
            List<PrintFile> printFiles
    ) {
        if (kafkaMessage == null) {
            logger.error("[buildPayload] kafkaMessage is null. Returning empty payload. batchId={}, fileName={}", batchId, fileName);
            return new SummaryPayload();
        }
        if (processedList == null) {
            logger.warn("[buildPayload] processedList is null. Using empty list. batchId={}, fileName={}", batchId, fileName);
            processedList = Collections.emptyList();
        }
        if (errorMap == null) {
            logger.warn("[buildPayload] errorMap is null. Defaulting to empty map. batchId={}, fileName={}", batchId, fileName);
            errorMap = Collections.emptyMap();
        }
        if (printFiles == null) {
            logger.warn("[buildPayload] printFiles is null. Using empty list. batchId={}, fileName={}", batchId, fileName);
            printFiles = Collections.emptyList();
        }

        logger.info("[GT] Start building payload. batchId={}, fileName={}, processedListSize={}, printFilesSize={}",
                batchId, fileName, processedList.size(), printFiles.size());

        SummaryPayload payload = new SummaryPayload();
        payload.setBatchID(batchId);
        payload.setFileName(fileName);
        payload.setTimestamp(timestamp);

        // --- Header ---
        Header header = new Header();
        header.setTenantCode(kafkaMessage.getTenantCode());
        header.setChannelID(kafkaMessage.getChannelID());
        header.setAudienceID(kafkaMessage.getAudienceID());
        header.setTimestamp(timestamp);
        header.setSourceSystem(kafkaMessage.getSourceSystem());
        header.setProduct(kafkaMessage.getSourceSystem());
        header.setJobName(kafkaMessage.getSourceSystem());
        payload.setHeader(header);

        // --- Processed files ---
        List<ProcessedFileEntry> processedFileEntries = buildProcessedFileEntries(processedList, errorMap, printFiles);
        payload.setProcessedFileList(processedFileEntries);

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

        // --- Payload Info ---
        Payload payloadInfo = new Payload();
        payloadInfo.setUniqueECPBatchRef(kafkaMessage.getUniqueECPBatchRef());
        payloadInfo.setRunPriority(kafkaMessage.getRunPriority());
        payloadInfo.setEventID(kafkaMessage.getEventID());
        payloadInfo.setEventType(kafkaMessage.getEventType());
        payloadInfo.setRestartKey(kafkaMessage.getRestartKey());
        payloadInfo.setFileCount(totalUniqueFiles);
        payload.setPayload(payloadInfo);

        // --- Metadata ---
        Metadata metadata = new Metadata();

        long totalArchiveEntries = processedFileEntries.stream()
                .filter(pf -> isNonEmpty(pf.getArchiveBlobUrl())).distinct()
                .count();
        metadata.setTotalCustomersProcessed((int) totalArchiveEntries);

        Set<String> statuses = processedFileEntries.stream()
                .map(ProcessedFileEntry::getOverallStatus)
                .filter(Objects::nonNull)
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

        logger.info("[GT] Metadata built. batchId={}, fileName={}, totalCustomers={}, overallStatus={}",
                batchId, fileName, metadata.getTotalCustomersProcessed(), overallStatus);

        // --- Print Files (Step 1: Assign status) ---
        for (PrintFile pf : printFiles) {
            if (pf == null) {
                logger.debug("[buildPayload] Skipping null PrintFile. batchId={}, fileName={}", batchId, fileName);
                continue;
            }
            String psUrl = pf.getPrintFileURL();

            if (psUrl != null && psUrl.endsWith(".ps")) {
                pf.setPrintStatus("SUCCESS");
            } else if (psUrl != null && errorMap.containsKey(psUrl)) {
                pf.setPrintStatus("FAILED");
            } else {
                pf.setPrintStatus("");
            }

            logger.debug("[GT] PrintFile processed. batchId={}, fileName={}, psUrl={}, status={}",
                    batchId, fileName, psUrl, pf.getPrintStatus());
        }

        // --- Print Files (Step 2: Decode + collect) ---
        List<PrintFile> printFileList = new ArrayList<>();
        for (PrintFile pf : printFiles) {
            if (pf != null && pf.getPrintFileURL() != null) {
                String decodedUrl = URLDecoder.decode(pf.getPrintFileURL(), StandardCharsets.UTF_8);

                PrintFile printFile = new PrintFile();
                printFile.setPrintFileURL(decodedUrl);
                printFile.setPrintStatus(pf.getPrintStatus() != null ? pf.getPrintStatus() : "");

                printFileList.add(printFile);
            }
        }
        payload.setPrintFiles(printFileList);

        logger.info("[GT] Completed building payload. batchId={}, fileName={}, processedEntries={}, printFiles={}",
                batchId, fileName, processedFileEntries.size(), printFileList.size());

        return payload;
    }
