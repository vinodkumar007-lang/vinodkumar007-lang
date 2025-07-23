private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorMap) {

    Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedFiles) {
        String key = file.getCustomerId() + "-" + file.getAccountNumber();
        ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());
        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());

        OutputFile output = new OutputFile();
        output.setOutputType(file.getOutputType());
        output.setBlobUrl(file.getBlobUrl());
        output.setStatus(file.getStatus());

        List<OutputFile> outputs = entry.getOutputFiles();
        if (outputs == null) outputs = new ArrayList<>();

        // Avoid duplicate outputType
        boolean alreadyAdded = outputs.stream()
                .anyMatch(o -> o.getOutputType().equalsIgnoreCase(file.getOutputType()));
        if (!alreadyAdded) {
            outputs.add(output);
        }

        entry.setOutputFiles(outputs);
        grouped.put(key, entry);
    }

    // ✅ Add overall status per record after building
    for (ProcessedFileEntry entry : grouped.values()) {
        List<OutputFile> files = entry.getOutputFiles();
        int successCount = 0;
        int failedCount = 0;
        int notFoundCount = 0;
        boolean errorMapAccountExists = false;

        for (OutputFile f : files) {
            String status = f.getStatus();
            if ("SUCCESS".equalsIgnoreCase(status)) {
                successCount++;
            } else if ("FAILED".equalsIgnoreCase(status)) {
                failedCount++;
            } else {
                notFoundCount++; // status is ""
            }
        }

        // 🔍 Check if this account exists in errorMap
        String errKey = entry.getCustomerId() + "-" + entry.getAccountNumber();
        Map<String, String> errDetails = errorMap.get(errKey);
        errorMapAccountExists = errDetails != null && !errDetails.isEmpty();

        // ✅ Check if any missing method matches errormap → mark as failed
        if (errorMapAccountExists) {
            for (OutputFile f : files) {
                if ((f.getBlobUrl() == null || f.getBlobUrl().isEmpty())
                        && (f.getStatus() == null || f.getStatus().isEmpty())) {
                    String errorMsg = errDetails.get(f.getOutputType());
                    if (errorMsg != null) {
                        f.setStatus("FAILED");
                        failedCount++;
                        notFoundCount--;
                    }
                }
            }
        }

        // ✅ Re-evaluate counts after patching statuses
        successCount = 0;
        failedCount = 0;
        notFoundCount = 0;
        for (OutputFile f : files) {
            String status = f.getStatus();
            if ("SUCCESS".equalsIgnoreCase(status)) {
                successCount++;
            } else if ("FAILED".equalsIgnoreCase(status)) {
                failedCount++;
            } else {
                notFoundCount++;
            }
        }

        // ✅ Determine final overall status
        String overallStatus;
        if (failedCount > 0) {
            overallStatus = "PARTIAL";
        } else if (errorMapAccountExists && failedCount == 0 && successCount > 0 && notFoundCount > 0) {
            overallStatus = "PARTIAL";
        } else if (successCount > 0 && failedCount == 0 && notFoundCount >= 0) {
            overallStatus = "SUCCESS";
        } else {
            overallStatus = "FAILED";
        }

        entry.setOverallStatus(overallStatus);
    }

    return new ArrayList<>(grouped.values());
}
