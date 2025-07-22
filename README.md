private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorMap) {

    Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedFiles) {
        String key = file.getCustomerId() + "-" + file.getAccountNumber();
        ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());

        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());

        String outputType = file.getOutputType() != null ? file.getOutputType().toUpperCase(Locale.ROOT) : "";
        String blobUrl = file.getBlobUrl();
        String status = file.getStatus();

        String errorKey = file.getCustomerId() + "-" + file.getAccountNumber();
        boolean isErrorPresent = errorMap.containsKey(errorKey);

        // ✅ If blobUrl is null or empty, clear status unless error exists (in that case mark FAILED)
        if (blobUrl == null || blobUrl.trim().isEmpty()) {
            if (isErrorPresent && !outputType.equals("ARCHIVE")) {
                status = "FAILED";
            } else {
                status = "";
            }
        }

        switch (outputType) {
            case "EMAIL":
                entry.setEmailBlobUrl(blobUrl);
                entry.setEmailStatus(status);
                break;
            case "ARCHIVE":
                entry.setArchiveBlobUrl(blobUrl);
                entry.setArchiveStatus(status);
                break;
            case "PRINT":
                entry.setPrintBlobUrl(blobUrl);
                entry.setPrintStatus(status);
                break;
            case "MOBSTAT":
                entry.setMobstatBlobUrl(blobUrl);
                entry.setMobstatStatus(status);
                break;
        }

        grouped.put(key, entry);
    }

    // ✅ Add missing error-only entries
    for (String errorKey : errorMap.keySet()) {
        if (!grouped.containsKey(errorKey)) {
            String[] parts = errorKey.split("-");
            if (parts.length == 2) {
                ProcessedFileEntry errorEntry = new ProcessedFileEntry();
                errorEntry.setCustomerId(parts[0]);
                errorEntry.setAccountNumber(parts[1]);
                errorEntry.setEmailStatus("FAILED");
                errorEntry.setOverallStatus("FAILED");
                grouped.put(errorKey, errorEntry);
            }
        }
    }

    // ✅ Final loop to calculate overallStatus
    for (ProcessedFileEntry entry : grouped.values()) {
        String errorKey = entry.getCustomerId() + "-" + entry.getAccountNumber();
        boolean isErrorPresent = errorMap.containsKey(errorKey);

        List<String> statuses = new ArrayList<>();
        if (entry.getEmailStatus() != null && !entry.getEmailStatus().isEmpty()) statuses.add(entry.getEmailStatus());
        if (entry.getMobstatStatus() != null && !entry.getMobstatStatus().isEmpty()) statuses.add(entry.getMobstatStatus());
        if (entry.getPrintStatus() != null && !entry.getPrintStatus().isEmpty()) statuses.add(entry.getPrintStatus());
        if (entry.getArchiveStatus() != null && !entry.getArchiveStatus().isEmpty()) statuses.add(entry.getArchiveStatus());

        boolean allSuccess = !statuses.isEmpty() && statuses.stream().allMatch(s -> "SUCCESS".equalsIgnoreCase(s));
        boolean anyFailed = statuses.stream().anyMatch(s -> "FAILED".equalsIgnoreCase(s));
        boolean allFailed = !statuses.isEmpty() && statuses.stream().allMatch(s -> "FAILED".equalsIgnoreCase(s));

        String overallStatus;

        if (isErrorPresent) {
            overallStatus = "FAILED";
        } else if (allSuccess) {
            overallStatus = "SUCCESS";
        } else if (allFailed) {
            overallStatus = "FAILED";
        } else if (anyFailed) {
            overallStatus = "PARTIAL";
        } else if ("SUCCESS".equalsIgnoreCase(entry.getArchiveStatus()) && statuses.size() == 1) {
            overallStatus = "SUCCESS";
        } else {
            overallStatus = "FAILED";
        }

        entry.setOverallStatus(overallStatus);
    }

    // ✅ Correct file count: only count if blobUrl exists and status is SUCCESS
    long fileCount = grouped.values().stream()
            .flatMap(entry -> Stream.of(
                    new AbstractMap.SimpleEntry<>(entry.getEmailBlobUrl(), entry.getEmailStatus()),
                    new AbstractMap.SimpleEntry<>(entry.getPrintBlobUrl(), entry.getPrintStatus()),
                    new AbstractMap.SimpleEntry<>(entry.getMobstatBlobUrl(), entry.getMobstatStatus()),
                    new AbstractMap.SimpleEntry<>(entry.getArchiveBlobUrl(), entry.getArchiveStatus())
            ))
            .filter(e -> e.getKey() != null && !e.getKey().trim().isEmpty()
                      && "SUCCESS".equalsIgnoreCase(e.getValue()))
            .count();

    System.out.println("✅ Final fileCount (blobUrls with SUCCESS): " + fileCount);

    return new ArrayList<>(grouped.values());
}
