public static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedList) {
    List<ProcessedFileEntry> finalList = new ArrayList<>();

    Map<String, List<SummaryProcessedFile>> grouped = processedList.stream()
            .collect(Collectors.groupingBy(file -> file.getCustomerId() + "::" + file.getAccountNumber()));

    for (Map.Entry<String, List<SummaryProcessedFile>> entry : grouped.entrySet()) {
        String[] keyParts = entry.getKey().split("::");
        String customerId = keyParts[0];
        String accountNumber = keyParts[1];
        List<SummaryProcessedFile> files = entry.getValue();

        Optional<SummaryProcessedFile> emailOpt = files.stream()
                .filter(f -> "EMAIL".equalsIgnoreCase(f.getOutputType()))
                .findFirst();
        Optional<SummaryProcessedFile> mobstatOpt = files.stream()
                .filter(f -> "MOBSTAT".equalsIgnoreCase(f.getOutputType()))
                .findFirst();
        Optional<SummaryProcessedFile> archiveOpt = files.stream()
                .filter(f -> "ARCHIVE".equalsIgnoreCase(f.getOutputType()))
                .findFirst();

        // EMAIL + ARCHIVE entry
        if (emailOpt.isPresent()) {
            ProcessedFileEntry emailEntry = new ProcessedFileEntry();
            emailEntry.setCustomerId(customerId);
            emailEntry.setAccountNumber(accountNumber);

            SummaryProcessedFile email = emailOpt.get();
            emailEntry.setPdfEmailFileUrl(email.getBlobUrl());
            emailEntry.setPdfEmailFileUrlStatus(email.getStatus());

            archiveOpt.ifPresent(archive -> {
                emailEntry.setArchiveBlobUrl(archive.getBlobUrl());
                emailEntry.setArchiveStatus(archive.getStatus());
            });

            emailEntry.setOverallStatus(computeOverallStatus(emailOpt.get(), archiveOpt.orElse(null)));
            finalList.add(emailEntry);
        }

        // MOBSTAT + ARCHIVE entry
        if (mobstatOpt.isPresent()) {
            ProcessedFileEntry mobEntry = new ProcessedFileEntry();
            mobEntry.setCustomerId(customerId);
            mobEntry.setAccountNumber(accountNumber);

            SummaryProcessedFile mob = mobstatOpt.get();
            mobEntry.setPdfMobstatFileUrl(mob.getBlobUrl());
            mobEntry.setPdfMobstatFileUrlStatus(mob.getStatus());

            archiveOpt.ifPresent(archive -> {
                mobEntry.setArchiveBlobUrl(archive.getBlobUrl());
                mobEntry.setArchiveStatus(archive.getStatus());
            });

            mobEntry.setOverallStatus(computeOverallStatus(mobstatOpt.get(), archiveOpt.orElse(null)));
            finalList.add(mobEntry);
        }
    }

    return finalList;
}
=========

private static String computeOverallStatus(SummaryProcessedFile primary, SummaryProcessedFile archive) {
    boolean primarySuccess = primary != null && "SUCCESS".equalsIgnoreCase(primary.getStatus());
    boolean archiveSuccess = archive != null && "SUCCESS".equalsIgnoreCase(archive.getStatus());

    if (primarySuccess && archiveSuccess) return "SUCCESS";
    if (!primarySuccess && archiveSuccess) return "FAILED";
    if (primarySuccess && !archiveSuccess) return "PARTIAL";
    return "FAILED";
}

===================
