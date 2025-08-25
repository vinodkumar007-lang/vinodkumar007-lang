    private static List<ProcessedFileEntry> buildProcessedFileEntries(
            List<SummaryProcessedFile> processedFiles,
            Map<String, Map<String, String>> errorMap,
            List<PrintFile> printFiles) {

        List<ProcessedFileEntry> allEntries = new ArrayList<>();

        for (SummaryProcessedFile file : processedFiles) {
            ProcessedFileEntry entry = new ProcessedFileEntry();
            entry.setCustomerId(file.getCustomerId());
            entry.setAccountNumber(file.getAccountNumber());

            // Map URLs and statuses
            entry.setEmailBlobUrl(file.getPdfEmailFileUrl());
            entry.setMobstatBlobUrl(file.getPdfMobstatFileUrl());
            entry.setPrintBlobUrl(file.getPrintFileUrl());
            entry.setArchiveBlobUrl(file.getArchiveBlobUrl());

            // Determine delivery status using blob URLs and error map
            Map<String, String> errors = errorMap.getOrDefault(file.getAccountNumber(), Collections.emptyMap());

            entry.setEmailStatus(isNonEmpty(file.getPdfEmailFileUrl()) ? "SUCCESS" :
                    "FAILED".equalsIgnoreCase(errors.getOrDefault("EMAIL", "")) ? "FAILED" : "");
            entry.setMobstatStatus(isNonEmpty(file.getPdfMobstatFileUrl()) ? "SUCCESS" :
                    "FAILED".equalsIgnoreCase(errors.getOrDefault("MOBSTAT", "")) ? "FAILED" : "");
            entry.setPrintStatus(isNonEmpty(file.getPrintFileUrl()) ? "SUCCESS" :
                    "FAILED".equalsIgnoreCase(errors.getOrDefault("PRINT", "")) ? "FAILED" : "");
            entry.setArchiveStatus(isNonEmpty(file.getArchiveBlobUrl()) ? "SUCCESS" :
                    "FAILED".equalsIgnoreCase(errors.getOrDefault("ARCHIVE", "")) ? "FAILED" : "");

            // Overall status logic
            boolean emailSuccess = "SUCCESS".equals(entry.getEmailStatus());
            boolean mobstatSuccess = "SUCCESS".equals(entry.getMobstatStatus());
            boolean printSuccess = "SUCCESS".equals(entry.getPrintStatus());
            boolean archiveSuccess = "SUCCESS".equals(entry.getArchiveStatus());

            boolean emailFailed = !emailSuccess;
            boolean mobstatFailed = !mobstatSuccess;
            boolean printFailed = !printSuccess;

            if ((emailSuccess && archiveSuccess) ||
                    (mobstatSuccess && archiveSuccess && emailFailed && printFailed) ||
                    (printSuccess && archiveSuccess && emailFailed && mobstatFailed)) {
                entry.setOverallStatus("SUCCESS");
            } else if (archiveSuccess) {
                entry.setOverallStatus("PARTIAL");
            } else {
                entry.setOverallStatus("FAILED");
            }

            // If error map exists, mark PARTIAL if not already FAILED
            if (errorMap.containsKey(entry.getAccountNumber()) && !"FAILED".equals(entry.getOverallStatus())) {
                entry.setOverallStatus("PARTIAL");
            }

            allEntries.add(entry);
        }

        return allEntries;
    }
