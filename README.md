private Map<String, List<SummaryProcessedFile>> buildDetailedProcessedFiles(
        List<ProcessedFileEntry> processedFileEntries,
        Map<String, ErrorDetail> errorDetailMap
) {
    Map<String, List<SummaryProcessedFile>> groupedFiles = new LinkedHashMap<>();

    for (ProcessedFileEntry entry : processedFileEntries) {
        SummaryProcessedFile summaryFile = new SummaryProcessedFile();
        summaryFile.setType(entry.getType());
        summaryFile.setUrl(entry.getBlobUrl());

        // Attach error info if available
        String errorKey = entry.getCustomerId() + "::" + entry.getAccountNumber() + "::" + entry.getType();
        ErrorDetail errorDetail = errorDetailMap.get(errorKey);

        if (entry.getBlobUrl() != null && !entry.getBlobUrl().isEmpty()) {
            summaryFile.setStatus("success");
        } else if (errorDetail != null) {
            summaryFile.setStatus("failed");
            summaryFile.setErrorCode(errorDetail.getErrorCode());
            summaryFile.setErrorMessage(errorDetail.getErrorMessage());
        } else {
            summaryFile.setStatus("not_found");
        }

        String groupKey = entry.getCustomerId() + "::" + entry.getAccountNumber() + "::" + entry.getOutputMethod();
        groupedFiles.computeIfAbsent(groupKey, k -> new ArrayList<>()).add(summaryFile);
    }

    // Add overallStatus per group
    for (Map.Entry<String, List<SummaryProcessedFile>> group : groupedFiles.entrySet()) {
        List<SummaryProcessedFile> files = group.getValue();
        boolean hasFailed = files.stream().anyMatch(f -> "failed".equalsIgnoreCase(f.getStatus()));
        boolean hasNotFound = files.stream().anyMatch(f -> "not_found".equalsIgnoreCase(f.getStatus()));

        String overallStatus;
        if (hasFailed) {
            overallStatus = "failed";
        } else if (hasNotFound) {
            overallStatus = "partial";
        } else {
            overallStatus = "success";
        }

        files.forEach(f -> f.setOverallStatus(overallStatus));
    }

    return groupedFiles;
}


public class ErrorReportEntry {

    private String customerId;
    private String accountNumber;
    private String outputMethod;
    private String errorCode;
    private String errorMessage;

    public ErrorReportEntry() {
    }

    public ErrorReportEntry(String customerId, String accountNumber, String outputMethod,
                            String errorCode, String errorMessage) {
        this.customerId = customerId;
        this.accountNumber = accountNumber;
        this.outputMethod = outputMethod;
        this.errorCode = errorCode;
        this.errorMessage = errorMessage;
    }

    public String getCustomerId() {
        return customerId;
    }

    public void setCustomerId(String customerId) {
        this.customerId = customerId;
    }

    public String getAccountNumber() {
        return accountNumber;
    }

    public void setAccountNumber(String accountNumber) {
        this.accountNumber = accountNumber;
    }

    public String getOutputMethod() {
        return outputMethod;
    }

    public void setOutputMethod(String outputMethod) {
        this.outputMethod = outputMethod;
    }

    public String getErrorCode() {
        return errorCode;
    }

    public void setErrorCode(String errorCode) {
        this.errorCode = errorCode;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public void setErrorMessage(String errorMessage) {
        this.errorMessage = errorMessage;
    }

    @Override
    public String toString() {
        return "ErrorReportEntry{" +
                "customerId='" + customerId + '\'' +
                ", accountNumber='" + accountNumber + '\'' +
                ", outputMethod='" + outputMethod + '\'' +
                ", errorCode='" + errorCode + '\'' +
                ", errorMessage='" + errorMessage + '\'' +
                '}';
    }
}
