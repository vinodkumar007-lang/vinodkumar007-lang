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
                case "MOBSTAT":
                    entry.setPdfMobstatFileUrl(file.getBlobURL());
                    entry.setPdfMobstatFileUrlStatus(file.getStatus());
                    break;
                case "PRINT":
                    entry.setPrintFileUrl(file.getBlobURL());
                    entry.setPrintFileUrlStatus(file.getStatus());
                    break;
                case "ARCHIVE":
                    entry.setArchiveBlobUrl(file.getBlobURL());
                    entry.setArchiveStatus(file.getStatus());
                    break;
            }

            grouped.put(key, entry);
        }

        // Apply final overallStatus per grouped entry
        for (ProcessedFileEntry entry : grouped.values()) {
            boolean archiveOk = "SUCCESS".equalsIgnoreCase(entry.getArchiveStatus());

            // EMAIL logic
            String emailStatus = entry.getPdfEmailFileUrlStatus();
            boolean emailSuccess = "SUCCESS".equalsIgnoreCase(emailStatus);
            boolean emailFailed = "FAILED".equalsIgnoreCase(emailStatus);

            // MOBSTAT logic
            String mobstatStatus = entry.getPdfMobstatFileUrlStatus();
            boolean mobstatSuccess = "SUCCESS".equalsIgnoreCase(mobstatStatus);

            // PRINT logic
            String printStatus = entry.getPrintFileUrlStatus();
            boolean printSuccess = "SUCCESS".equalsIgnoreCase(printStatus);

            // If EMAIL found
            if (emailSuccess && archiveOk) {
                entry.setOverallStatus("SUCCESS");
            } else if (emailFailed) {
                entry.setOverallStatus("FAILED");
            } else if (emailStatus == null && archiveOk) {
                entry.setOverallStatus("SUCCESS");
            } else if (!archiveOk && (emailFailed || emailStatus == null)) {
                entry.setOverallStatus("FAILED");
            } else {
                entry.setOverallStatus("PARTIAL");
            }

            // Extend to other types if needed (optional override for MOBSTAT/PRINT only cases)
            if (entry.getPdfEmailFileUrlStatus() == null && entry.getPdfMobstatFileUrlStatus() != null && mobstatSuccess && archiveOk) {
                entry.setOverallStatus("SUCCESS");
            }
            if (entry.getPdfEmailFileUrlStatus() == null && entry.getPdfEmailFileUrl() != null && printSuccess && archiveOk) {
                entry.setOverallStatus("SUCCESS");
            }
        }
