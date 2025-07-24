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
            status = "";  // ✅ Replaced "NOT_FOUND" with ""
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

    // ✅ Overall status logic (unchanged)
    for (ProcessedFileEntry entry : grouped.values()) {
        String email = entry.getEmailStatus();
        String print = entry.getPrintStatus();
        String mobstat = entry.getMobstatStatus();
        String archive = entry.getArchiveStatus();

        boolean isEmailSuccess = "SUCCESS".equals(email);
        boolean isPrintSuccess = "SUCCESS".equals(print);
        boolean isMobstatSuccess = "SUCCESS".equals(mobstat);
        boolean isArchiveSuccess = "SUCCESS".equals(archive);

        boolean isEmailMissingOrFailed = email == null || "FAILED".equals(email) || "".equals(email);
        boolean isPrintMissingOrFailed = print == null || "FAILED".equals(print) || "".equals(print);
        boolean isMobstatMissingOrFailed = mobstat == null || "FAILED".equals(mobstat) || "".equals(mobstat);

        if (isEmailSuccess && isArchiveSuccess) {
            entry.setOverallStatus("SUCCESS");
        } else if (isMobstatSuccess && isArchiveSuccess && isEmailMissingOrFailed && isPrintMissingOrFailed) {
            entry.setOverallStatus("SUCCESS");
        } else if (isPrintSuccess && isArchiveSuccess && isEmailMissingOrFailed && isMobstatMissingOrFailed) {
            entry.setOverallStatus("SUCCESS");
        } else if (isArchiveSuccess && isEmailMissingOrFailed && isMobstatMissingOrFailed && isPrintMissingOrFailed) {
            entry.setOverallStatus("PARTIAL");
        } else if (isArchiveSuccess) {
            entry.setOverallStatus("PARTIAL");
        } else {
            entry.setOverallStatus("FAILED");
        }
    }

    return new ArrayList<>(grouped.values());
}
