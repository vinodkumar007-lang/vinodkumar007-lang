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
                .filter(entry -> "SUCCESS".equalsIgnoreCase(entry.getOverAllStatusCode()))
                .count();
        long failed = processedFileEntries.stream()
                .filter(entry -> "FAILED".equalsIgnoreCase(entry.getOverAllStatusCode()))
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

    private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedList) {
        List<ProcessedFileEntry> finalList = new ArrayList<>();

        Map<String, List<SummaryProcessedFile>> grouped = processedList.stream()
                .filter(f -> f.getCustomerId() != null && f.getAccountNumber() != null)
                .collect(Collectors.groupingBy(f -> f.getCustomerId() + "::" + f.getAccountNumber()));

        for (Map.Entry<String, List<SummaryProcessedFile>> group : grouped.entrySet()) {
            String[] parts = group.getKey().split("::");
            String customerId = parts[0];
            String accountNumber = parts[1];

            Map<String, SummaryProcessedFile> methodMap = new HashMap<>();
            Map<String, SummaryProcessedFile> archiveMap = new HashMap<>();

            for (SummaryProcessedFile file : group.getValue()) {
                String method = file.getOutputMethod();
                if (method == null) continue;

                switch (method.toUpperCase()) {
                    case "EMAIL", "MOBSTAT", "PRINT" -> methodMap.put(method.toUpperCase(), file);
                    case "ARCHIVE" -> {
                        String linked = file.getLinkedDeliveryType();
                        if (linked != null) {
                            archiveMap.put(linked.toUpperCase(), file);
                        }
                    }
                }
            }

            ProcessedFileEntry entry = new ProcessedFileEntry();
            entry.setCustomerId(customerId);
            entry.setAccountNumber(accountNumber);

            List<String> statuses = new ArrayList<>();
            boolean hasSuccess = false;

            for (String type : List.of("EMAIL", "MOBSTAT", "PRINT")) {
                SummaryProcessedFile delivery = methodMap.get(type);
                SummaryProcessedFile archive = archiveMap.get(type);

                String deliveryStatus = null, archiveStatus = null;

                if (delivery != null) {
                    String url = delivery.getBlobURL();
                    deliveryStatus = delivery.getStatus();
                    String reason = delivery.getStatusDescription();

                    switch (type) {
                        case "EMAIL" -> {
                            entry.setPdfEmailFileUrl(url);
                            entry.setPdfEmailFileUrlStatus(deliveryStatus);
                            if ("FAILED".equalsIgnoreCase(deliveryStatus)) entry.setReason(reason);
                        }
                        case "MOBSTAT" -> {
                            entry.setPdfMobstatFileUrl(url);
                            entry.setPdfMobstatFileUrlStatus(deliveryStatus);
                            if ("FAILED".equalsIgnoreCase(deliveryStatus)) entry.setReason(reason);
                        }
                        case "PRINT" -> {
                            entry.setPrintFileUrl(url);
                            entry.setPrintFileUrlStatus(deliveryStatus);
                            if ("FAILED".equalsIgnoreCase(deliveryStatus)) entry.setReason(reason);
                        }
                    }

                    if ("SUCCESS".equalsIgnoreCase(deliveryStatus)) hasSuccess = true;
                }

                if (archive != null) {
                    archiveStatus = archive.getStatus();
                    String aUrl = archive.getBlobURL();

                    entry.setPdfArchiveFileUrl(aUrl); // shared field
                    entry.setPdfArchiveFileUrlStatus(archiveStatus);

                    if ("FAILED".equalsIgnoreCase(archiveStatus) && entry.getReason() == null) {
                        entry.setReason(archive.getStatusDescription());
                    }

                    if ("SUCCESS".equalsIgnoreCase(archiveStatus)) hasSuccess = true;
                }

                // Record method-level status only if at least one file is present
                if (deliveryStatus != null || archiveStatus != null) {
                    if ("SUCCESS".equalsIgnoreCase(deliveryStatus) && "SUCCESS".equalsIgnoreCase(archiveStatus)) {
                        statuses.add("SUCCESS");
                    } else if ("FAILED".equalsIgnoreCase(deliveryStatus) && "FAILED".equalsIgnoreCase(archiveStatus)) {
                        statuses.add("FAILED");
                    } else {
                        statuses.add("PARTIAL");
                    }
                }
            }

            // ✅ Skip if no file for this customer is SUCCESS
            if (!hasSuccess) continue;

            // ✅ Determine overall status
            if (statuses.stream().allMatch(s -> "SUCCESS".equals(s))) {
                entry.setOverAllStatusCode("SUCCESS");
            } else if (statuses.stream().allMatch(s -> "FAILED".equals(s))) {
                entry.setOverAllStatusCode("FAILED");
            } else {
                entry.setOverAllStatusCode("PARTIAL");
            }

            finalList.add(entry);
        }

        return finalList;
    }
