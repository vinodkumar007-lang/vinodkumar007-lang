private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedList,
        Map<String, Map<String, String>> errorMap) {

    List<ProcessedFileEntry> finalList = new ArrayList<>();

    // Define mapping of grouped output types to primary label
    Map<Set<String>, String> groupTypeToLabel = new HashMap<>();
    groupTypeToLabel.put(new HashSet<>(Arrays.asList("EMAIL", "ARCHIVE")), "EMAIL");
    groupTypeToLabel.put(new HashSet<>(Arrays.asList("PRINT", "ARCHIVE")), "PRINT");
    groupTypeToLabel.put(new HashSet<>(Arrays.asList("MOBSTAT", "ARCHIVE")), "MOBSTAT");
    groupTypeToLabel.put(new HashSet<>(Arrays.asList("HTML", "ARCHIVE")), "HTML"); // Add more if needed

    // Group by customerId + accountNumber
    Map<String, List<SummaryProcessedFile>> grouped =
            processedList.stream()
                    .filter(f -> f.getCustomerId() != null && f.getAccountNumber() != null)
                    .collect(Collectors.groupingBy(f ->
                            f.getCustomerId() + "::" + f.getAccountNumber()));

    for (Map.Entry<String, List<SummaryProcessedFile>> entry : grouped.entrySet()) {
        String[] parts = entry.getKey().split("::");
        String customerId = parts[0];
        String accountNumber = parts[1];

        List<SummaryProcessedFile> files = entry.getValue();

        // Build map of outputType -> blobUrl
        Map<String, String> typeToUrl = new HashMap<>();
        for (SummaryProcessedFile file : files) {
            typeToUrl.put(file.getOutputType(), file.getBlobUrl());
        }

        // For each defined group combination (EMAIL+ARCHIVE, PRINT+ARCHIVE, etc.)
        for (Map.Entry<Set<String>, String> groupEntry : groupTypeToLabel.entrySet()) {
            Set<String> combo = groupEntry.getKey(); // e.g. [EMAIL, ARCHIVE]
            String outputTypeLabel = groupEntry.getValue(); // e.g. EMAIL

            if (!typeToUrl.keySet().containsAll(combo)) {
                // If not present in actual uploaded outputTypes, skip
                continue;
            }

            // Check if all URLs are present
            boolean allSuccess = combo.stream().allMatch(t -> typeToUrl.get(t) != null);

            String status;
            if (allSuccess) {
                status = "SUCCESS";
            } else {
                boolean anyFailed = combo.stream().anyMatch(t -> {
                    String errKey = customerId + "::" + accountNumber;
                    return errorMap.containsKey(errKey) &&
                            errorMap.get(errKey).getOrDefault(t, "").equalsIgnoreCase("Failed");
                });

                if (anyFailed) {
                    status = "FAILED";
                } else {
                    status = "PARTIAL";
                }
            }

            // Pick primary URL (like EMAIL or PRINT), fallback to ARCHIVE if primary null
            String mainBlobUrl = typeToUrl.get(outputTypeLabel);
            if (mainBlobUrl == null && combo.contains("ARCHIVE")) {
                mainBlobUrl = typeToUrl.get("ARCHIVE");
            }

            ProcessedFileEntry processedFile = new ProcessedFileEntry();
            processedFile.setCustomerId(customerId);
            processedFile.setAccountNumber(accountNumber);
            processedFile.setOutputType(outputTypeLabel);
            processedFile.setBlobUrl(mainBlobUrl);
            processedFile.setStatus(status);

            finalList.add(processedFile);
        }
    }

    return finalList;
}
