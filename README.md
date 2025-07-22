private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedFiles) {
        Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

        for (SummaryProcessedFile file : processedFiles) {
            String key = file.getCustomerId() + "-" + file.getAccountNumber();
            ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());

            entry.setCustomerId(file.getCustomerId());
            entry.setAccountNumber(file.getAccountNumber());

            // Set archiveBlobUrl only once (from any record)
            if (entry.getArchiveBlobUrl() == null && file.getArchiveBlobUrl() != null) {
                entry.setArchiveBlobUrl(file.getArchiveBlobUrl());
                entry.setArchiveStatus(file.getArchiveStatus());
            }

            // Set individual delivery method outputs
            switch (file.getOutputType()) {
                case "EMAIL":
                    entry.setPdfEmailFileUrl(file.getBlobURL());
                    entry.setPdfEmailFileUrlStatus(file.getStatus());
                    break;
                case "MOBSTAT":
                    entry.setPdfMobstatFileUrl(file.getBlobURL());
                    entry.setPdfMobstatFileUrlStatus(file.getStatus());
                    break;
                case "PRINT":
                    // Optional: Add if needed
                    break;
            }

            grouped.put(key, entry);
        }

        // Set overallStatus for each grouped customer
        for (ProcessedFileEntry entry : grouped.values()) {
            String emailStatus = entry.getPdfEmailFileUrlStatus();
            String archiveStatus = entry.getArchiveStatus();
            String mobstatStatus = entry.getPdfMobstatFileUrlStatus();

            boolean emailOk = emailStatus == null || "SUCCESS".equalsIgnoreCase(emailStatus);
            boolean archiveOk = "SUCCESS".equalsIgnoreCase(archiveStatus);
            boolean mobstatOk = mobstatStatus == null || "SUCCESS".equalsIgnoreCase(mobstatStatus);

            boolean emailFailed = "FAILED".equalsIgnoreCase(emailStatus);

            // Logic based on combinations
            if (emailOk && archiveOk && mobstatOk) {
                entry.setOverallStatus("SUCCESS");
            } else if (emailFailed) {
                entry.setOverallStatus("FAILED");
            } else if (!emailOk && archiveOk) {
                entry.setOverallStatus("SUCCESS");
            } else if (!emailOk && !archiveOk) {
                entry.setOverallStatus("FAILED");
            } else {
                entry.setOverallStatus("PARTIAL");
            }
        }

        return new ArrayList<>(grouped.values());
    }
