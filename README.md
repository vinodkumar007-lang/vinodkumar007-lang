    private static List<ProcessedFileEntry> buildProcessedFileEntries(
            List<SummaryProcessedFile> processedFiles,
            Map<String, Map<String, String>> errorMap) {

        Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

        for (SummaryProcessedFile file : processedFiles) {
            String key = file.getCustomerId() + "-" + file.getAccountNumber();
            ProcessedFileEntry entry = grouped.computeIfAbsent(key, k -> {
                ProcessedFileEntry newEntry = new ProcessedFileEntry();
                newEntry.setCustomerId(file.getCustomerId());
                newEntry.setAccountNumber(file.getAccountNumber());
                return newEntry;
            });

            String outputType = file.getOutputType();
            String blobUrl = file.getBlobUrl();
            Map<String, String> errors = errorMap.getOrDefault(file.getAccountNumber(), Collections.emptyMap());

            String status;
            if (isNonEmpty(blobUrl)) {
                status = "SUCCESS";
            } else if ("FAILED".equalsIgnoreCase(errors.getOrDefault(outputType, ""))) {
                status = "FAILED";
            } else {
                status = "NOT_FOUND";
            }

            switch (outputType) {
                case "EMAIL" -> {
                    entry.setEmailBlobUrl(blobUrl);
                    entry.setEmailStatus(status);
                }
                case "PRINT" -> {
                    entry.setPrintFileUrl(blobUrl);
                    entry.setPrintStatus(status);
                }
                case "MOBSTAT" -> {
                    entry.setMobstatBlobUrl(blobUrl);
                    entry.setMobstatStatus(status);
                }
                case "ARCHIVE" -> {
                    entry.setArchiveBlobUrl(blobUrl);
                    entry.setArchiveStatus(status);
                }
            }
        }

        // Compute overallStatus
        for (ProcessedFileEntry entry : grouped.values()) {
            String email = entry.getEmailStatus();
            String archive = entry.getArchiveStatus();

            if ("SUCCESS".equals(email) && "SUCCESS".equals(archive)) {
                entry.setOverallStatus("SUCCESS");
            } else if ("FAILED".equals(email) && "FAILED".equals(archive)) {
                entry.setOverallStatus("FAILED");
            } else if ("SUCCESS".equals(archive) && ("FAILED".equals(email) || "NOT_FOUND".equals(email))) {
                entry.setOverallStatus("PARTIAL");
            } else if ("SUCCESS".equals(archive)) {
                entry.setOverallStatus("SUCCESS");
            } else {
                entry.setOverallStatus("FAILED");
            }
        }

        return new ArrayList<>(grouped.values());
    }
