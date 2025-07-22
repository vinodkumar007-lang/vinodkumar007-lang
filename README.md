private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedFiles) {
    Map<String, List<SummaryProcessedFile>> grouped = processedFiles.stream()
        .collect(Collectors.groupingBy(p -> p.getCustomerId() + "|" + p.getAccountNumber()));

    List<ProcessedFileEntry> result = new ArrayList<>();

    for (Map.Entry<String, List<SummaryProcessedFile>> entry : grouped.entrySet()) {
        List<SummaryProcessedFile> groupList = entry.getValue();
        if (groupList.isEmpty()) continue;

        SummaryProcessedFile first = groupList.get(0);
        ProcessedFileEntry processedEntry = new ProcessedFileEntry();
        processedEntry.setCustomerId(first.getCustomerId());
        processedEntry.setAccountNumber(first.getAccountNumber());

        boolean hasFailed = false;
        boolean hasPartial = false;

        for (SummaryProcessedFile file : groupList) {
            String type = file.getOutputType();
            String status = file.getStatus();

            switch (type) {
                case "EMAIL":
                    processedEntry.setPdfEmailFileUrl(file.getBlobURL());
                    processedEntry.setPdfEmailFileUrlStatus(status);
                    break;
                case "MOBSTAT":
                    processedEntry.setPdfMobstatFileUrl(file.getBlobURL());
                    processedEntry.setPdfMobstatFileUrlStatus(status);
                    break;
                case "PRINT":
                    processedEntry.setPrintFileUrl(file.getBlobURL());
                    processedEntry.setPrintFileUrlStatus(status);
                    break;
            }

            // Always add archive if available
            if (file.getArchiveBlobUrl() != null) {
                processedEntry.setArchiveBlobUrl(file.getArchiveBlobUrl());
                processedEntry.setArchiveStatus(file.getArchiveStatus());
            }

            if ("FAILED".equalsIgnoreCase(status)) {
                hasFailed = true;
            } else if ("NOT-FOUND".equalsIgnoreCase(status)) {
                hasPartial = true;
            }
        }

        if (hasFailed) {
            processedEntry.setOverallStatus("FAILED");
        } else if (hasPartial) {
            processedEntry.setOverallStatus("PARTIAL");
        } else {
            processedEntry.setOverallStatus("SUCCESS");
        }

        result.add(processedEntry);
    }

    return result;
}
