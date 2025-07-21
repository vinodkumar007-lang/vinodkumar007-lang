private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> errorMap,
        KafkaMessage message
) {
    List<SummaryProcessedFile> processedFiles = new ArrayList<>();
    Map<String, SummaryProcessedFile> outputMap = new HashMap<>();

    for (SummaryProcessedFile customer : customerList) {
        String customerId = customer.getCustomerId();
        String accountNumber = customer.getAccountNumber();

        for (String type : Arrays.asList("email", "print", "mobstat", "archive")) {
            try {
                // Build expected file name or directory path
                Path targetFolder = jobDir.resolve(type);
                Optional<Path> match = findCustomerFile(targetFolder, customerId, accountNumber);

                SummaryProcessedFile entry = new SummaryProcessedFile();
                entry.setCustomerId(customerId);
                entry.setAccountNumber(accountNumber);
                entry.setOutputType(type.toUpperCase());

                if (match.isPresent()) {
                    Path file = match.get();

                    // Upload file to blob
                    String blobUrl = blobStorageService.uploadFileAndReturnLocation(
                            file.toFile(),
                            message.getSourceSystem(),
                            message.getBatchId(),
                            type,
                            customerId,
                            accountNumber
                    );
                    entry.setBlobURL(blobUrl);
                    entry.setStatus("SUCCESS");
                } else {
                    entry.setStatus("ERROR");
                    entry.setBlobURL(null);
                }

                // Check if errors exist for this customer+account
                String key = customerId + "::" + accountNumber;
                if (errorMap.containsKey(key)) {
                    Map<String, String> details = errorMap.get(key);
                    List<ErrorDetail> errorDetails = details.entrySet().stream()
                            .map(e -> new ErrorDetail(e.getKey(), e.getValue()))
                            .collect(Collectors.toList());

                    ErrorReportEntry errorEntry = new ErrorReportEntry();
                    errorEntry.setCustomerId(customerId);
                    errorEntry.setAccountNumber(accountNumber);
                    errorEntry.setErrorDetails(errorDetails);

                    entry.setErrorReportEntry(errorEntry);
                }

                processedFiles.add(entry);

            } catch (Exception e) {
                logger.error("❌ Failed processing customer={} account={} type={}", customerId, accountNumber, type, e);
            }
        }
    }

    return processedFiles;
}

private Optional<Path> findCustomerFile(Path folder, String customerId, String accountNumber) {
    try (Stream<Path> files = Files.walk(folder)) {
        return files
                .filter(Files::isRegularFile)
                .filter(path -> {
                    String fileName = path.getFileName().toString().toLowerCase();
                    return fileName.contains(customerId.toLowerCase()) && fileName.contains(accountNumber.toLowerCase());
                })
                .findFirst();
    } catch (IOException e) {
        logger.warn("⚠️ Could not search files in folder: {}", folder);
        return Optional.empty();
    }
}

public class ErrorDetail {
    private String field;
    private String description;

    public ErrorDetail() {}

    public ErrorDetail(String field, String description) {
        this.field = field;
        this.description = description;
    }

    // Getters and setters
}

public class ErrorReportEntry {
    private String customerId;
    private String accountNumber;
    private List<ErrorDetail> errorDetails;

    // Getters and setters
}
