private Map<String, Map<String, String>> parseErrorReport(KafkaMessage msg) {
    Map<String, Map<String, String>> map = new HashMap<>();
    Path errorPath = Paths.get(mountPath, "output", msg.getSourceSystem(), msg.getJobName(), "ErrorReport.csv");

    if (!Files.exists(errorPath)) return map;

    try (BufferedReader reader = Files.newBufferedReader(errorPath)) {
        String line;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split("\\|");

            // Standardize logic for >= 3 fields
            if (parts.length >= 3) {
                String acc = parts[0].trim();
                String method = (parts.length >= 4 ? parts[3] : parts[2]).trim().toUpperCase();
                String status = (parts.length >= 5 ? parts[4] : (parts.length >= 4 ? parts[3] : "Failed")).trim();

                // Normalize missing/misplaced fields
                if (method.isEmpty()) method = "UNKNOWN";
                if (status.isEmpty()) status = "Failed";

                map.computeIfAbsent(acc, k -> new HashMap<>()).put(method, status);
            }
        }
    } catch (Exception e) {
        logger.warn("⚠️ Error reading ErrorReport.csv", e);
    }

    return map;
}

================
private static List<ProcessedFileEntry> buildDetailedProcessedFileEntries(
        List<SummaryProcessedFile> processedList,
        Map<String, Map<String, String>> errorMap) {

    List<ProcessedFileEntry> finalList = new ArrayList<>();

    Map<String, List<SummaryProcessedFile>> grouped = processedList.stream()
            .filter(f -> f.getCustomerId() != null && f.getAccountNumber() != null)
            .collect(Collectors.groupingBy(f -> f.getCustomerId() + "::" + f.getAccountNumber()));

    for (Map.Entry<String, List<SummaryProcessedFile>> entry : grouped.entrySet()) {
        String[] parts = entry.getKey().split("::");
        String customerId = parts[0];
        String accountNumber = parts[1];

        List<SummaryProcessedFile> customerFiles = entry.getValue();

        for (SummaryProcessedFile file : customerFiles) {
            String method = file.getOutputType().toUpperCase();
            String status = "SUCCESS"; // default

            Map<String, String> methodMap = errorMap.get(accountNumber);
            if (methodMap != null) {
                String result = methodMap.get(method);
                if ("Failed".equalsIgnoreCase(result)) {
                    status = "FAILED";
                } else if (result == null) {
                    status = "PARTIAL"; // method missing for this account = partial
                }
            }

            ProcessedFileEntry entryObj = new ProcessedFileEntry();
            entryObj.setCustomerId(customerId);
            entryObj.setAccountNumber(accountNumber);
            entryObj.setOutputType(method);
            entryObj.setBlobUrl(file.getBlobUrl());
            entryObj.setStatus(status);

            finalList.add(entryObj);
        }
    }

    return finalList;
}

===========
private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedList,
        Map<String, Map<String, String>> errorMap) {

    List<ProcessedFileEntry> finalList = new ArrayList<>();

    // Group by customerId + accountNumber
    Map<String, List<SummaryProcessedFile>> grouped = processedList.stream()
            .filter(f -> f.getCustomerId() != null && f.getAccountNumber() != null)
            .collect(Collectors.groupingBy(f -> f.getCustomerId() + "::" + f.getAccountNumber()));

    for (Map.Entry<String, List<SummaryProcessedFile>> entry : grouped.entrySet()) {
        String[] parts = entry.getKey().split("::");
        String customerId = parts[0];
        String accountNumber = parts[1];
        List<SummaryProcessedFile> files = entry.getValue();

        Map<String, String> methodMap = errorMap.getOrDefault(accountNumber, Collections.emptyMap());

        for (SummaryProcessedFile file : files) {
            String method = file.getOutputType() != null ? file.getOutputType().toUpperCase() : "";
            String status = "SUCCESS";

            if (errorMap.containsKey(accountNumber)) {
                if (methodMap.containsKey(method)) {
                    String result = methodMap.get(method);
                    if ("Failed".equalsIgnoreCase(result)) {
                        status = "FAILED";
                    }
                } else {
                    status = "PARTIAL"; // method missing under errorMap
                }
            }

            ProcessedFileEntry processed = new ProcessedFileEntry();
            processed.setCustomerId(customerId);
            processed.setAccountNumber(accountNumber);
            processed.setOutputType(method);
            processed.setBlobUrl(file.getBlobUrl());
            processed.setStatus(status);

            finalList.add(processed);
        }
    }

    return finalList;
}
