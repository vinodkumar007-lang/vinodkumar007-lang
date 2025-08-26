private static List<ProcessedFileEntry> buildProcessedFileEntries(
            List<SummaryProcessedFile> processedFiles,
            Map<String, Map<String, String>> errorMap,
            List<PrintFile> ignoredPrintFiles) {

        List<ProcessedFileEntry> allEntries = new ArrayList<>();

        for (SummaryProcessedFile file : processedFiles) {
            if (file == null) continue;

            ProcessedFileEntry entry = new ProcessedFileEntry();
            entry.setCustomerId(file.getCustomerId());
            entry.setAccountNumber(file.getAccountNumber());
            entry.setEmailBlobUrl(file.getPdfEmailFileUrl());
            entry.setMobstatBlobUrl(file.getPdfMobstatFileUrl());
            entry.setPrintBlobUrl(file.getPrintFileUrl());
            entry.setArchiveBlobUrl(file.getArchiveBlobUrl());

            Map<String, String> errors = errorMap.getOrDefault(file.getAccountNumber(), Collections.emptyMap());

            entry.setEmailStatus(isNonEmpty(file.getPdfEmailFileUrl()) ? "SUCCESS" :
                    "FAILED".equalsIgnoreCase(errors.getOrDefault("EMAIL", "")) ? "FAILED" : "");
            entry.setMobstatStatus(isNonEmpty(file.getPdfMobstatFileUrl()) ? "SUCCESS" :
                    "FAILED".equalsIgnoreCase(errors.getOrDefault("MOBSTAT", "")) ? "FAILED" : "");
            entry.setPrintStatus(isNonEmpty(file.getPrintFileUrl()) ? "SUCCESS" :
                    "FAILED".equalsIgnoreCase(errors.getOrDefault("PRINT", "")) ? "FAILED" : "");
            entry.setArchiveStatus(isNonEmpty(file.getArchiveBlobUrl()) ? "SUCCESS" :
                    "FAILED".equalsIgnoreCase(errors.getOrDefault("ARCHIVE", "")) ? "FAILED" : "");

            boolean emailSuccess = "SUCCESS".equals(entry.getEmailStatus());
            boolean mobstatSuccess = "SUCCESS".equals(entry.getMobstatStatus());
            boolean printSuccess = "SUCCESS".equals(entry.getPrintStatus());
            boolean archiveSuccess = "SUCCESS".equals(entry.getArchiveStatus());

            if ((emailSuccess && archiveSuccess) ||
                    (mobstatSuccess && archiveSuccess && !emailSuccess && !printSuccess) ||
                    (printSuccess && archiveSuccess && !emailSuccess && !mobstatSuccess)) {
                entry.setOverallStatus("SUCCESS");
            } else if (archiveSuccess) {
                entry.setOverallStatus("PARTIAL");
            } else {
                entry.setOverallStatus("FAILED");
            }

            if (errorMap.containsKey(file.getAccountNumber()) && !"FAILED".equals(entry.getOverallStatus())) {
                entry.setOverallStatus("PARTIAL");
            }

            allEntries.add(entry);
        }

        return allEntries;
    }
