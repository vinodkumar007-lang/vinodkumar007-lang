    private static List<ProcessedFileEntry> buildProcessedFileEntries(
            List<SummaryProcessedFile> processedFiles,
            Map<String, Map<String, String>> errorReportMap
    ) {
        Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

        for (SummaryProcessedFile file : processedFiles) {
            String key = file.getCustomerId() + "-" + file.getAccountNumber();
            ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());

            entry.setCustomerId(file.getCustomerId());
            entry.setAccountNumber(file.getAccountNumber());

            switch (file.getOutputType()) {
                case "EMAIL":
                    entry.setEmailBlobUrl(file.getBlobUrl());
                    entry.setEmailStatus(file.getStatus());
                    break;
                case "ARCHIVE":
                    entry.setArchiveBlobUrl(file.getBlobUrl());
                    entry.setArchiveStatus(file.getStatus());
                    break;
                case "PRINT":
                    entry.setPrintBlobUrl(file.getBlobUrl());
                    entry.setPrintStatus(file.getStatus());
                    break;
                case "MOBSTAT":
                    entry.setMobstatBlobUrl(file.getBlobUrl());
                    entry.setMobstatStatus(file.getStatus());
                    break;
            }

            grouped.put(key, entry);
        }

        // Finalize statuses
        for (ProcessedFileEntry entry : grouped.values()) {
            boolean archiveSuccess = "SUCCESS".equalsIgnoreCase(entry.getArchiveStatus());

            boolean emailFailed = isFailedOrMissing(entry.getEmailStatus(), entry.getCustomerId(), entry.getAccountNumber(), "EMAIL", errorReportMap);
            boolean printFailed = isFailedOrMissing(entry.getPrintStatus(), entry.getCustomerId(), entry.getAccountNumber(), "PRINT", errorReportMap);
            boolean mobstatFailed = isFailedOrMissing(entry.getMobstatStatus(), entry.getCustomerId(), entry.getAccountNumber(), "MOBSTAT", errorReportMap);

            boolean anyFailed = emailFailed || printFailed || mobstatFailed;
            boolean allMissing = isAllMissing(entry);

            if (!archiveSuccess) {
                entry.setOverallStatus("FAILED");
            } else if (anyFailed) {
                entry.setOverallStatus("FAILED"); // You can switch this to "PARTIAL" if your rule permits
            } else if (archiveSuccess) {
                entry.setOverallStatus("SUCCESS");
            } else {
                entry.setOverallStatus("FAILED");
            }
        }

        return new ArrayList<>(grouped.values());
    }
