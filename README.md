private static ProcessedFileEntry mapToProcessedFileEntry(SummaryProcessedFile file, Map<String, String> errors) {
    ProcessedFileEntry entry = new ProcessedFileEntry();

    // 🔹 Handle Dummy → "" safely
    String customerId = file.getCustomerId();
    if (customerId == null) {
        customerId = ""; // null-safe fallback
    } else if ("Dummy".equalsIgnoreCase(customerId.trim())) {
        customerId = ""; // replace Dummy with empty string
    }
    entry.setCustomerId(customerId);

    entry.setAccountNumber(file.getAccountNumber());
    entry.setEmailBlobUrl(file.getPdfEmailFileUrl());
    entry.setMobstatBlobUrl(file.getPdfMobstatFileUrl());
    entry.setPrintBlobUrl(file.getPrintFileUrl());
    entry.setArchiveBlobUrl(file.getArchiveBlobUrl());

    entry.setEmailStatus(isNonEmpty(file.getPdfEmailFileUrl()) ? "SUCCESS" :
            "FAILED".equalsIgnoreCase(errors.getOrDefault("EMAIL", "")) ? "FAILED" : "");
    entry.setMobstatStatus(isNonEmpty(file.getPdfMobstatFileUrl()) ? "SUCCESS" :
            "FAILED".equalsIgnoreCase(errors.getOrDefault("MOBSTAT", "")) ? "FAILED" : "");
    entry.setPrintStatus(isNonEmpty(file.getPrintFileUrl()) ? "SUCCESS" :
            "FAILED".equalsIgnoreCase(errors.getOrDefault("PRINT", "")) ? "FAILED" : "");
    entry.setArchiveStatus(isNonEmpty(file.getArchiveBlobUrl()) ? "SUCCESS" :
            "FAILED".equalsIgnoreCase(errors.getOrDefault("ARCHIVE", "")) ? "FAILED" : "");

    return entry;
}
