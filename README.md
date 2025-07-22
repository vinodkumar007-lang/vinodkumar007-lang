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

        String emailStatus = null, mobstatStatus = null, printStatus = null, archiveStatus = null;
        String emailUrl = null, mobstatUrl = null, printUrl = null, archiveUrl = null;

        for (SummaryProcessedFile file : groupList) {
            switch (file.getOutputType()) {
                case "EMAIL":
                    emailStatus = file.getStatus();
                    emailUrl = file.getBlobURL();
                    break;
                case "MOBSTAT":
                    mobstatStatus = file.getStatus();
                    mobstatUrl = file.getBlobURL();
                    break;
                case "PRINT":
                    printStatus = file.getStatus();
                    printUrl = file.getBlobURL();
                    break;
            }

            if (file.getArchiveBlobUrl() != null) {
                archiveUrl = file.getArchiveBlobUrl();
                archiveStatus = file.getArchiveStatus();
            }
        }

        // Set individual URLs and statuses
        processedEntry.setPdfEmailFileUrl(emailUrl);
        processedEntry.setPdfEmailFileUrlStatus(emailStatus);
        processedEntry.setPdfMobstatFileUrl(mobstatUrl);
        processedEntry.setPdfMobstatFileUrlStatus(mobstatStatus);
        processedEntry.setPrintFileUrl(printUrl);
        processedEntry.setPrintFileUrlStatus(printStatus);
        processedEntry.setArchiveBlobUrl(archiveUrl);
        processedEntry.setArchiveStatus(archiveStatus);

        // Determine overall status
        boolean hasFailure = false;
        boolean hasPartial = false;

        // EMAIL
        if (emailStatus == null && emailUrl == null) {
            hasPartial = true;
        } else if ("FAILED".equalsIgnoreCase(emailStatus)) {
            hasFailure = true;
        }

        // MOBSTAT
        if (mobstatStatus == null && mobstatUrl == null) {
            hasPartial = true;
        } else if ("FAILED".equalsIgnoreCase(mobstatStatus)) {
            hasFailure = true;
        }

        // PRINT
        if (printStatus == null && printUrl == null) {
            hasPartial = true;
        } else if ("FAILED".equalsIgnoreCase(printStatus)) {
            hasFailure = true;
        }

        // ARCHIVE — always must exist
        if (archiveStatus == null || !"SUCCESS".equalsIgnoreCase(archiveStatus)) {
            hasFailure = true; // archive is mandatory
        }

        // Final decision
        if (hasFailure) {
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
