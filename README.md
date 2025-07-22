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
            case "ARCHIVE":
                entry.setArchiveBlobUrl(file.getBlobURL());
                entry.setArchiveStatus(file.getStatus());
                break;
            case "MOBSTAT":
                entry.setPdfMobstatFileUrl(file.getBlobURL());
                entry.setPdfMobstatFileUrlStatus(file.getStatus());
                break;
            // PRINT is intentionally excluded
        }

        grouped.put(key, entry);
    }

    for (ProcessedFileEntry entry : grouped.values()) {
        String emailStatus = entry.getPdfEmailFileUrlStatus();
        String archiveStatus = entry.getArchiveStatus();
        String mobstatStatus = entry.getPdfMobstatFileUrlStatus();

        boolean emailPresent = emailStatus != null;
        boolean archivePresent = archiveStatus != null;

        boolean emailSuccess = "SUCCESS".equalsIgnoreCase(emailStatus);
        boolean archiveSuccess = "SUCCESS".equalsIgnoreCase(archiveStatus);
        boolean mobstatSuccess = mobstatStatus == null || "SUCCESS".equalsIgnoreCase(mobstatStatus);

        // Determine overall status logic
        if (emailSuccess && archiveSuccess && mobstatSuccess) {
            entry.setOverallStatus("SUCCESS");
        } else if (emailPresent && "FAILED".equalsIgnoreCase(emailStatus)) {
            entry.setOverallStatus("FAILED");
        } else if (!emailPresent && archiveSuccess) {
            entry.setOverallStatus("SUCCESS");
        } else if (!emailPresent && !archiveSuccess) {
            entry.setOverallStatus("FAILED");
        } else {
            entry.setOverallStatus("PARTIAL");
        }
    }

    return new ArrayList<>(grouped.values());
}
