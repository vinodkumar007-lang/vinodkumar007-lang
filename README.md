private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorMap,
        List<PrintFile> printFiles) {

    List<ProcessedFileEntry> result = new ArrayList<>();
    Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedFiles) {
        String key = file.getCustomerId() + "-" + file.getAccountNumber();

        // For ARCHIVE → create a fresh entry for every archive file (don’t group)
        if ("ARCHIVE".equalsIgnoreCase(file.getOutputType())) {
            ProcessedFileEntry archiveEntry = new ProcessedFileEntry();
            archiveEntry.setCustomerId(file.getCustomerId());
            archiveEntry.setAccountNumber(file.getAccountNumber());
            archiveEntry.setArchiveBlobUrl(file.getBlobUrl());
            archiveEntry.setArchiveStatus(isNonEmpty(file.getBlobUrl()) ? "SUCCESS" : "FAILED");

            // Apply status rules (similar to your grouped logic but archive-only)
            if ("SUCCESS".equals(archiveEntry.getArchiveStatus())) {
                archiveEntry.setOverallStatus("PARTIAL"); // archive-only = partial
            } else {
                archiveEntry.setOverallStatus("FAILED");
            }

            // Final override if account has errors
            if (errorMap.containsKey(file.getAccountNumber())
                    && !"FAILED".equals(archiveEntry.getOverallStatus())) {
                archiveEntry.setOverallStatus("PARTIAL");
            }

            result.add(archiveEntry);
            continue; // skip grouped handling
        }

        // For EMAIL, PRINT, MOBSTAT → group as before
        ProcessedFileEntry entry = grouped.computeIfAbsent(key, k -> {
            ProcessedFileEntry newEntry = new ProcessedFileEntry();
            newEntry.setCustomerId(file.getCustomerId());
            newEntry.setAccountNumber(file.getAccountNumber());
            return newEntry;
        });

        String outputType = file.getOutputType();
        String blobUrl = file.getBlobUrl();
        Map<String, String> errors = errorMap.getOrDefault(file.getAccountNumber(), Collections.emptyMap());

        // Determine delivery status
        String status;
        if (isNonEmpty(blobUrl)) {
            status = "SUCCESS";
        } else if ("FAILED".equalsIgnoreCase(errors.getOrDefault(outputType, ""))) {
            status = "FAILED";
        } else {
            status = "";
        }

        switch (outputType) {
            case "EMAIL" -> {
                entry.setEmailBlobUrl(blobUrl);
                entry.setEmailStatus(status);
            }
            case "PRINT" -> {
                if (printFiles != null && !printFiles.isEmpty()) {
                    entry.setPrintStatus("SUCCESS");
                } else {
                    entry.setPrintStatus("");
                }
            }
            case "MOBSTAT" -> {
                entry.setMobstatBlobUrl(blobUrl);
                entry.setMobstatStatus(status);
            }
        }
    }

    // Apply overall status logic for grouped entries
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

        if (errorMap.containsKey(entry.getAccountNumber())
                && !"FAILED".equals(entry.getOverallStatus())) {
            entry.setOverallStatus("PARTIAL");
        }

        result.add(entry);
    }

    return result;
}
