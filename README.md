 private static List<ProcessedFileEntry> buildProcessedFileEntries(
            List<SummaryProcessedFile> processedList,
            Map<String, Map<String, String>> errorMap // key = customerId::accountNumber, value = Map<outputType, reason>
    ) {
        List<ProcessedFileEntry> finalList = new ArrayList<>();

        // Group by customerId::accountNumber
        Map<String, List<SummaryProcessedFile>> grouped = processedList.stream()
                .filter(f -> f.getCustomerId() != null && f.getAccountNumber() != null)
                .collect(Collectors.groupingBy(f -> f.getCustomerId() + "::" + f.getAccountNumber()));

        for (Map.Entry<String, List<SummaryProcessedFile>> entry : grouped.entrySet()) {
            String[] parts = entry.getKey().split("::");
            String customerId = parts[0];
            String accountNumber = parts[1];
            List<SummaryProcessedFile> records = entry.getValue();

            // Map of outputType -> SummaryProcessedFile
            Map<String, SummaryProcessedFile> typeMap = records.stream()
                    .collect(Collectors.toMap(SummaryProcessedFile::getOutputType, f -> f, (a, b) -> a));

            SummaryProcessedFile archive = typeMap.get("ARCHIVE");
            if (archive == null) continue; // skip if archive missing

            for (String outputMethod : List.of("EMAIL", "PRINT", "MOBSTAT")) {
                SummaryProcessedFile output = typeMap.get(outputMethod);

                ProcessedFileEntry entryObj = new ProcessedFileEntry();
                entryObj.setCustomerId(customerId);
                entryObj.setAccountNumber(accountNumber);
                entryObj.setOutputMethod(outputMethod);

                // Archive info
                entryObj.setArchiveBlobUrl(archive.getBlobURL());
                entryObj.setArchiveStatus(archive.getStatus());

                // Output method info
                if (output != null) {
                    entryObj.setOutputBlobUrl(output.getBlobURL());
                    entryObj.setOutputStatus(output.getStatus());
                } else {
                    String key = customerId + "::" + accountNumber;
                    Map<String, String> failedOutputs = errorMap.getOrDefault(key, Collections.emptyMap());

                    entryObj.setOutputBlobUrl(null);
                    if (failedOutputs.containsKey(outputMethod)) {
                        entryObj.setOutputStatus("FAILED");
                    } else {
                        entryObj.setOutputStatus("NOT-FOUND");
                    }
                }

                // Overall status
                switch (entryObj.getOutputStatus()) {
                    case "SUCCESS":
                        entryObj.setOverallStatus("SUCCESS");
                        break;
                    case "FAILED":
                        entryObj.setOverallStatus("FAILED");
                        break;
                    case "NOT-FOUND":
                    default:
                        entryObj.setOverallStatus("PARTIAL");
                        break;
                }

                finalList.add(entryObj);
            }
        }

        return finalList;
    }
