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

    public static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedList) {
        List<ProcessedFileEntry> finalList = new ArrayList<>();

        Map<String, List<SummaryProcessedFile>> grouped = processedList.stream()
                .collect(Collectors.groupingBy(file -> file.getCustomerId() + "::" + file.getAccountNumber()));

        for (Map.Entry<String, List<SummaryProcessedFile>> entry : grouped.entrySet()) {
            String[] keyParts = entry.getKey().split("::");
            String customerId = keyParts[0];
            String accountNumber = keyParts[1];
            List<SummaryProcessedFile> files = entry.getValue();

            // Separate by type
            Optional<SummaryProcessedFile> emailOpt = files.stream()
                    .filter(f -> "EMAIL".equalsIgnoreCase(f.getOutputType()))
                    .findFirst();
            Optional<SummaryProcessedFile> mobstatOpt = files.stream()
                    .filter(f -> "MOBSTAT".equalsIgnoreCase(f.getOutputType()))
                    .findFirst();
            Optional<SummaryProcessedFile> printOpt = files.stream()
                    .filter(f -> "PRINT".equalsIgnoreCase(f.getOutputType()))
                    .findFirst();
            Optional<SummaryProcessedFile> archiveOpt = files.stream()
                    .filter(f -> "ARCHIVE".equalsIgnoreCase(f.getOutputType()))
                    .findFirst();

            // EMAIL + ARCHIVE combo
            if (emailOpt.isPresent() || archiveOpt.isPresent()) {
                ProcessedFileEntry emailEntry = new ProcessedFileEntry();
                emailEntry.setCustomerId(customerId);
                emailEntry.setAccountNumber(accountNumber);

                SummaryProcessedFile email = emailOpt.orElse(null);
                SummaryProcessedFile archive = archiveOpt.orElse(null);

                if (email != null) {
                    emailEntry.setOutputType("EMAIL");
                    emailEntry.setBlobUrl(email.getPdfEmailFileUrl());
                    emailEntry.setStatus(email.getStatus());
                }

                if (archive != null) {
                    emailEntry.setArchiveBlobUrl(archive.getArchiveBlobUrl());
                    emailEntry.setArchiveStatus(archive.getStatus());
                }

                emailEntry.setOverallStatus(computeOverallStatus(email, archive));
                finalList.add(emailEntry);
            }

            // MOBSTAT + ARCHIVE combo
            if (mobstatOpt.isPresent()) {
                ProcessedFileEntry mobstatEntry = new ProcessedFileEntry();
                mobstatEntry.setCustomerId(customerId);
                mobstatEntry.setAccountNumber(accountNumber);

                SummaryProcessedFile mobstat = mobstatOpt.get();
                mobstatEntry.setOutputType("MOBSTAT");
                mobstatEntry.setBlobUrl(mobstat.getPdfMobstatFileUrl());
                mobstatEntry.setStatus(mobstat.getStatus());

                archiveOpt.ifPresent(archive -> {
                    mobstatEntry.setArchiveBlobUrl(archive.getArchiveBlobUrl());
                    mobstatEntry.setArchiveStatus(archive.getStatus());
                });

                mobstatEntry.setOverallStatus(computeOverallStatus(mobstatOpt.get(), archiveOpt.orElse(null)));
                finalList.add(mobstatEntry);
            }

            // PRINT + ARCHIVE combo
            if (printOpt.isPresent()) {
                ProcessedFileEntry printEntry = new ProcessedFileEntry();
                printEntry.setCustomerId(customerId);
                printEntry.setAccountNumber(accountNumber);

                SummaryProcessedFile print = printOpt.get();
                printEntry.setOutputType("PRINT");
                printEntry.setBlobUrl(print.getArchiveBlobUrl());
                printEntry.setStatus(print.getStatus());

                archiveOpt.ifPresent(archive -> {
                    printEntry.setArchiveBlobUrl(archive.getArchiveBlobUrl());
                    printEntry.setArchiveStatus(archive.getStatus());
                });

                printEntry.setOverallStatus(computeOverallStatus(printOpt.get(), archiveOpt.orElse(null)));
                finalList.add(printEntry);
            }
        }

        return finalList;
    }
