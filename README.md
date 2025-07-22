public static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedList) {
    List<ProcessedFileEntry> finalList = new ArrayList<>();

    Map<String, List<SummaryProcessedFile>> grouped = processedList.stream()
        .collect(Collectors.groupingBy(file -> file.getCustomerId() + "::" + file.getAccountNumber()));

    for (Map.Entry<String, List<SummaryProcessedFile>> entry : grouped.entrySet()) {
        String[] keyParts = entry.getKey().split("::");
        String customerId = keyParts[0];
        String accountNumber = keyParts[1];
        List<SummaryProcessedFile> files = entry.getValue();

        // Separate by type
        Optional<SummaryProcessedFile> emailOpt = files.stream()
            .filter(f -> "EMAIL".equalsIgnoreCase(f.getOutputType()))
            .findFirst();
        Optional<SummaryProcessedFile> mobstatOpt = files.stream()
            .filter(f -> "MOBSTAT".equalsIgnoreCase(f.getOutputType()))
            .findFirst();
        Optional<SummaryProcessedFile> printOpt = files.stream()
            .filter(f -> "PRINT".equalsIgnoreCase(f.getOutputType()))
            .findFirst();
        Optional<SummaryProcessedFile> archiveOpt = files.stream()
            .filter(f -> "ARCHIVE".equalsIgnoreCase(f.getOutputType()))
            .findFirst();

        // EMAIL + ARCHIVE combo
        if (emailOpt.isPresent() || archiveOpt.isPresent()) {
            ProcessedFileEntry emailEntry = new ProcessedFileEntry();
            emailEntry.setCustomerId(customerId);
            emailEntry.setAccountNumber(accountNumber);

            SummaryProcessedFile email = emailOpt.orElse(null);
            SummaryProcessedFile archive = archiveOpt.orElse(null);

            if (email != null) {
                emailEntry.setOutputType("EMAIL");
                emailEntry.setBlobUrl(email.getBlobUrl());
                emailEntry.setStatus(email.getStatus());
            }

            if (archive != null) {
                emailEntry.setArchiveBlobUrl(archive.getBlobUrl());
                emailEntry.setArchiveStatus(archive.getStatus());
            }

            emailEntry.setOverallStatus(computeOverallStatus(email, archive));
            finalList.add(emailEntry);
        }

        // MOBSTAT + ARCHIVE combo
        if (mobstatOpt.isPresent()) {
            ProcessedFileEntry mobstatEntry = new ProcessedFileEntry();
            mobstatEntry.setCustomerId(customerId);
            mobstatEntry.setAccountNumber(accountNumber);

            SummaryProcessedFile mobstat = mobstatOpt.get();
            mobstatEntry.setOutputType("MOBSTAT");
            mobstatEntry.setBlobUrl(mobstat.getBlobUrl());
            mobstatEntry.setStatus(mobstat.getStatus());

            archiveOpt.ifPresent(archive -> {
                mobstatEntry.setArchiveBlobUrl(archive.getBlobUrl());
                mobstatEntry.setArchiveStatus(archive.getStatus());
            });

            mobstatEntry.setOverallStatus(computeOverallStatus(mobstatOpt.get(), archiveOpt.orElse(null)));
            finalList.add(mobstatEntry);
        }

        // PRINT + ARCHIVE combo
        if (printOpt.isPresent()) {
            ProcessedFileEntry printEntry = new ProcessedFileEntry();
            printEntry.setCustomerId(customerId);
            printEntry.setAccountNumber(accountNumber);

            SummaryProcessedFile print = printOpt.get();
            printEntry.setOutputType("PRINT");
            printEntry.setBlobUrl(print.getBlobUrl());
            printEntry.setStatus(print.getStatus());

            archiveOpt.ifPresent(archive -> {
                printEntry.setArchiveBlobUrl(archive.getBlobUrl());
                printEntry.setArchiveStatus(archive.getStatus());
            });

            printEntry.setOverallStatus(computeOverallStatus(printOpt.get(), archiveOpt.orElse(null)));
            finalList.add(printEntry);
        }
    }

    return finalList;
}

private static String computeOverallStatus(SummaryProcessedFile primary, SummaryProcessedFile archive) {
    boolean primarySuccess = primary != null && "SUCCESS".equalsIgnoreCase(primary.getStatus());
    boolean archiveSuccess = archive != null && "SUCCESS".equalsIgnoreCase(archive.getStatus());

    if (primarySuccess && archiveSuccess) return "SUCCESS";
    if (!primarySuccess && archiveSuccess) return "FAILED";
    if (primarySuccess && !archiveSuccess) return "PARTIAL";
    return "FAILED";
}

