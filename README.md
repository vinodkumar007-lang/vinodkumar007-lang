private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedList, Map<String, ErrorReportEntry> errorMap) {
    Map<String, List<SummaryProcessedFile>> grouped = processedList.stream()
        .collect(Collectors.groupingBy(f -> f.getCustomerId() + ":" + f.getAccountNumber() + ":" + f.getOutputType()));

    List<ProcessedFileEntry> result = new ArrayList<>();

    grouped.forEach((key, list) -> {
        SummaryProcessedFile mainFile = list.stream()
            .filter(f -> !"ARCHIVE".equalsIgnoreCase(f.getOutputType()))
            .findFirst()
            .orElse(null);

        SummaryProcessedFile archiveFile = list.stream()
            .filter(f -> "ARCHIVE".equalsIgnoreCase(f.getOutputType()))
            .findFirst()
            .orElse(null);

        if (mainFile != null) {
            ProcessedFileEntry entry = new ProcessedFileEntry();
            entry.setCustomerId(mainFile.getCustomerId());
            entry.setAccountNumber(mainFile.getAccountNumber());
            entry.setOutputType(mainFile.getOutputType());
            entry.setFileUrl(mainFile.getBlobUrl());

            if (mainFile.getBlobUrl() != null) {
                entry.setStatus("SUCCESS");
            } else {
                String errorKey = mainFile.getCustomerId() + ":" + mainFile.getAccountNumber();
                if (errorMap.containsKey(errorKey)) {
                    entry.setStatus("FAILED");
                } else {
                    return; // Skip adding this entry
                }
            }

            if (archiveFile != null && archiveFile.getBlobUrl() != null) {
                entry.setArchiveBlobUrl(archiveFile.getBlobUrl());
                entry.setArchiveStatus("SUCCESS");
            } else {
                entry.setArchiveStatus("FAILED");
            }

            // Set overall status
            if ("SUCCESS".equals(entry.getStatus()) && "SUCCESS".equals(entry.getArchiveStatus())) {
                entry.setOverallStatus("SUCCESS");
            } else if ("FAILED".equals(entry.getStatus()) && "SUCCESS".equals(entry.getArchiveStatus())) {
                entry.setOverallStatus("FAILED");
            } else {
                entry.setOverallStatus("PARTIAL");
            }

            result.add(entry);
        }
    });

    return result;
}
