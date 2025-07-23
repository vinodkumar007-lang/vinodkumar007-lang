private static List<ProcessedFileEntry> buildProcessedFileEntries(
        List<SummaryProcessedFile> processedFiles,
        Map<String, Map<String, String>> errorMap) {

    Map<String, ProcessedFileEntry> grouped = new LinkedHashMap<>();

    for (SummaryProcessedFile file : processedFiles) {
        String key = file.getCustomerId() + "-" + file.getAccountNumber();
        ProcessedFileEntry entry = grouped.getOrDefault(key, new ProcessedFileEntry());

        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());

        // Add file type and status
        entry.getFileDetails().put(file.getMethod(), new FileStatus(file.getFileUrl(), "SUCCESS"));

        grouped.put(key, entry);
    }

    for (Map.Entry<String, ProcessedFileEntry> groupEntry : grouped.entrySet()) {
        String key = groupEntry.getKey();
        ProcessedFileEntry entry = groupEntry.getValue();

        String[] parts = key.split("-");
        String customerId = parts.length > 0 ? parts[0] : "";
        String accountNumber = parts.length > 1 ? parts[1] : "";

        entry.setCustomerId(customerId);
        entry.setAccountNumber(accountNumber);

        Map<String, FileStatus> fileStatuses = entry.getFileDetails();

        // Ensure all 3 delivery methods are populated
        String[] methods = {"EMAIL", "MOBSTAT", "PRINT", "ARCHIVE"};
        for (String method : methods) {
            if (!fileStatuses.containsKey(method)) {
                String status = "NA";

                // Only apply errorMap to EMAIL/MOBSTAT/PRINT (not ARCHIVE)
                if (!method.equals("ARCHIVE")) {
                    Map<String, String> methodErrors = errorMap.getOrDefault(key, new HashMap<>());
                    if (methodErrors.containsKey(method)) {
                        String error = methodErrors.get(method);
                        if ("NOT_FOUND".equals(error)) {
                            status = "FAILED";
                        } else {
                            status = "FAILED"; // All errors treated as failure
                        }
                    }
                }

                fileStatuses.put(method, new FileStatus(null, status));
            }
        }

        // Compute overallStatus
        String overallStatus = "SUCCESS";
        for (Map.Entry<String, FileStatus> statusEntry : fileStatuses.entrySet()) {
            String method = statusEntry.getKey();
            String status = statusEntry.getValue().getStatus();

            if (!"NA".equals(status) && !"SUCCESS".equals(status)) {
                overallStatus = "PARTIAL";
                break;
            }
        }

        entry.setOverallStatus(overallStatus);
    }

    return new ArrayList<>(grouped.values());
}

public class FileStatus {
    private String fileUrl;
    private String status;

    public FileStatus(String fileUrl, String status) {
        this.fileUrl = fileUrl;
        this.status = status;
    }

    // Getters and setters
    public String getFileUrl() {
        return fileUrl;
    }

    public void setFileUrl(String fileUrl) {
        this.fileUrl = fileUrl;
    }

    public String getStatus() {
        return status;
    }

    public void setStatus(String status) {
        this.status = status;
    }
}
