private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorMap) {

    Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

    // Step 1: Group all processed file entries by customerId + accountNumber
    for (SummaryProcessedFile file : processedFiles) {
        String key = file.getCustomerId() + "-" + file.getAccountNumber();
        ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());

        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());

        String deliveryMethod = file.getDeliveryType();
        String status = file.getStatus();
        String fileUrl = file.getBlobFileUrl();

        if ("EMAIL".equalsIgnoreCase(deliveryMethod)) {
            entry.setEmailStatus(status);
            entry.setEmailFileUrl(fileUrl);
        } else if ("PRINT".equalsIgnoreCase(deliveryMethod)) {
            entry.setPrintStatus(status);
            entry.setPrintFileUrl(fileUrl);
        } else if ("MOBSTAT".equalsIgnoreCase(deliveryMethod)) {
            entry.setMobstatStatus(status);
            entry.setMobstatFileUrl(fileUrl);
        } else if ("ARCHIVE".equalsIgnoreCase(deliveryMethod)) {
            entry.setArchiveStatus(status);
            entry.setArchiveFileUrl(fileUrl);
        }

        grouped.put(key, entry);
    }

    // Step 2: Apply error map statuses (FAILED or NOT_FOUND)
    for (ProcessedFileEntry entry : grouped.values()) {
        String key = entry.getCustomerId() + "-" + entry.getAccountNumber();
        Map<String, String> customerErrors = errorMap.getOrDefault(key, new HashMap<>());

        if (entry.getEmailStatus() == null) {
            if (customerErrors.containsKey("EMAIL")) {
                entry.setEmailStatus("FAILED");
            } else {
                entry.setEmailStatus("NOT_FOUND");
            }
        }

        if (entry.getPrintStatus() == null) {
            if (customerErrors.containsKey("PRINT")) {
                entry.setPrintStatus("FAILED");
            } else {
                entry.setPrintStatus("NOT_FOUND");
            }
        }

        if (entry.getMobstatStatus() == null) {
            if (customerErrors.containsKey("MOBSTAT")) {
                entry.setMobstatStatus("FAILED");
            } else {
                entry.setMobstatStatus("NOT_FOUND");
            }
        }

        if (entry.getArchiveStatus() == null) {
            if (customerErrors.containsKey("ARCHIVE")) {
                entry.setArchiveStatus("FAILED");
            } else {
                entry.setArchiveStatus("NOT_FOUND");
            }
        }
    }

    // Step 3: Assign "NA" to statuses that are NOT_FOUND but not expected (i.e., truly not requested)
    for (ProcessedFileEntry entry : grouped.values()) {
        if ("NOT_FOUND".equals(entry.getEmailStatus()) &&
                entry.getEmailFileUrl() == null &&
                !errorMap.getOrDefault(entry.getCustomerId() + "-" + entry.getAccountNumber(), new HashMap<>())
                        .containsKey("EMAIL")) {
            entry.setEmailStatus("NA");
        }

        if ("NOT_FOUND".equals(entry.getPrintStatus()) &&
                entry.getPrintFileUrl() == null &&
                !errorMap.getOrDefault(entry.getCustomerId() + "-" + entry.getAccountNumber(), new HashMap<>())
                        .containsKey("PRINT")) {
            entry.setPrintStatus("NA");
        }

        if ("NOT_FOUND".equals(entry.getMobstatStatus()) &&
                entry.getMobstatFileUrl() == null &&
                !errorMap.getOrDefault(entry.getCustomerId() + "-" + entry.getAccountNumber(), new HashMap<>())
                        .containsKey("MOBSTAT")) {
            entry.setMobstatStatus("NA");
        }
    }

    // Step 4: Compute overallStatus
    for (ProcessedFileEntry entry : grouped.values()) {
        String email = entry.getEmailStatus();
        String print = entry.getPrintStatus();
        String mobstat = entry.getMobstatStatus();
        String archive = entry.getArchiveStatus();

        boolean isEmailSuccess = "SUCCESS".equals(email);
        boolean isPrintSuccess = "SUCCESS".equals(print);
        boolean isMobstatSuccess = "SUCCESS".equals(mobstat);
        boolean isArchiveSuccess = "SUCCESS".equals(archive);

        boolean isEmailMissingOrFailed = "FAILED".equals(email) || "NOT_FOUND".equals(email);
        boolean isPrintMissingOrFailed = "FAILED".equals(print) || "NOT_FOUND".equals(print);
        boolean isMobstatMissingOrFailed = "FAILED".equals(mobstat) || "NOT_FOUND".equals(mobstat);

        boolean isEmailNA = "NA".equals(email);
        boolean isPrintNA = "NA".equals(print);
        boolean isMobstatNA = "NA".equals(mobstat);

        // ✅ SUCCESS: one delivery method + archive are SUCCESS, others are NA
        if (isEmailSuccess && isArchiveSuccess && isPrintNA && isMobstatNA) {
            entry.setOverallStatus("SUCCESS");
        } else if (isMobstatSuccess && isArchiveSuccess && isEmailNA && isPrintNA) {
            entry.setOverallStatus("SUCCESS");
        } else if (isPrintSuccess && isArchiveSuccess && isEmailNA && isMobstatNA) {
            entry.setOverallStatus("SUCCESS");
        }
        // ✅ PARTIAL: Archive is SUCCESS but other methods failed or missing (not NA)
        else if (isArchiveSuccess &&
                (isEmailMissingOrFailed || isMobstatMissingOrFailed || isPrintMissingOrFailed)) {
            entry.setOverallStatus("PARTIAL");
        }
        // ❌ Fully failed
        else {
            entry.setOverallStatus("FAILED");
        }
    }

    return new ArrayList<>(grouped.values());
}
