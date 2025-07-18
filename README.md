private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedList) {
        Map<String, ProcessedFileEntry> entryMap = new LinkedHashMap<>();
        Map<String, List<String>> statusTracker = new HashMap<>();

        for (SummaryProcessedFile file : processedList) {
            String customerId = file.getCustomerId();
            String accountNumber = file.getAccountNumber();
            String blobURL = file.getBlobURL();
            String status = file.getStatus() != null ? file.getStatus() : "UNKNOWN";

            // Skip if both URL and status are null or status is NOT_FOUND
            if ((blobURL == null && !"FAILED".equalsIgnoreCase(status)) ||
                    customerId == null || accountNumber == null) {
                continue;
            }

            String key = customerId + "::" + accountNumber;

            ProcessedFileEntry entry = entryMap.computeIfAbsent(key, k -> {
                ProcessedFileEntry e = new ProcessedFileEntry();
                e.setCustomerId(customerId);
                e.setAccountNumber(accountNumber);
                return e;
            });

            // Track status for overallStatus computation
            statusTracker.computeIfAbsent(key, k -> new ArrayList<>()).add(status);

            String lowerUrl = blobURL != null ? URLDecoder.decode(blobURL, StandardCharsets.UTF_8).toLowerCase() : "";

            if (lowerUrl.contains("/email/")) {
                entry.setPdfEmailFileUrl(blobURL);
                entry.setPdfEmailFileUrlStatus(status);
            } else if (lowerUrl.contains("/archive/")) {
                entry.setPdfArchiveFileUrl(blobURL);
                entry.setPdfArchiveFileUrlStatus(status);
            } else if (lowerUrl.contains("/mobstat/")) {
                entry.setPdfMobstatFileUrl(blobURL);
                entry.setPdfMobstatFileUrlStatus(status);
            } else if (lowerUrl.contains("/print/")) {
                entry.setPrintFileUrl(blobURL);
                entry.setPrintFileUrlStatus(status);
            } else {
                // If URL is null but status is FAILED (e.g., generation failure), track placeholder
                if ("FAILED".equalsIgnoreCase(status)) {
                    entry.setPdfArchiveFileUrl(null); // or skip setting fileUrl
                    entry.setPdfArchiveFileUrlStatus("FAILED");
                }
            }
        }

        // Set overallStatus per customer-account group
        for (Map.Entry<String, ProcessedFileEntry> groupedEntry : entryMap.entrySet()) {
            List<String> statuses = statusTracker.getOrDefault(groupedEntry.getKey(), List.of());

            String overallStatus;
            if (statuses.stream().allMatch(s -> "SUCCESS".equalsIgnoreCase(s))) {
                overallStatus = "SUCCESS";
            } else if (statuses.stream().anyMatch(s -> "FAILED".equalsIgnoreCase(s))) {
                overallStatus = "PARTIAL";
            } else {
                overallStatus = "UNKNOWN";
            }

            groupedEntry.getValue().setStatusCode(overallStatus);
        }

        return new ArrayList<>(entryMap.values());
    }
