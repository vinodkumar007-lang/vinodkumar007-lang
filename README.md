private List<SummaryProcessedFile> appendFailureEntries(String errorReportFilePath, Map<String, String> successMap) {
    List<SummaryProcessedFile> failures = new ArrayList<>();
    if (errorReportFilePath == null) return failures;

    Path path = Paths.get(errorReportFilePath);
    if (!Files.exists(path)) return failures;

    try (BufferedReader reader = Files.newBufferedReader(path)) {
        String line;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split("\\|");
            if (parts.length >= 4) {
                String account = parts[0].trim();
                String customer = parts[1].trim();
                String outputMethod = parts[2].trim().toUpperCase();
                String status = parts[3].trim();

                if (!successMap.containsKey(account) && "FAILED".equalsIgnoreCase(status)) {
                    SummaryProcessedFile failEntry = new SummaryProcessedFile();
                    failEntry.setAccountNumber(account);
                    failEntry.setCustomerId(customer);
                    failEntry.setStatusCode("FAILURE");
                    failEntry.setStatusDescription("Processing failed");

                    // Set FAILED URL for the appropriate delivery method
                    switch (outputMethod) {
                        case "EMAIL"   -> failEntry.setPdfEmailFileUrl("FAILED");
                        case "HTML"    -> failEntry.setHtmlEmailFileUrl("FAILED");
                        case "MOBSTAT" -> failEntry.setPdfMobstatFileUrl("FAILED");
                        case "TXT"     -> failEntry.setTxtEmailFileUrl("FAILED");
                        case "ARCHIVE" -> failEntry.setPdfArchiveFileUrl("FAILED");
                        default -> logger.warn("⚠️ Unknown OutputMethod in ErrorReport: {}", outputMethod);
                    }

                    failures.add(failEntry);
                }
            }
        }
    } catch (IOException e) {
        logger.error("❌ Failed to read error report file", e);
    }

    logger.info("📉 Appended {} failure entries from ErrorReport", failures.size());
    return failures;
}

List<SummaryProcessedFile> processedFiles = buildAndUploadProcessedFiles(jobDir, accountCustomerMap, message);

// Add error report failures (if any)
String errorReportPath = Paths.get(jobDir.toString(), "ErrorReport.csv").toString();
List<SummaryProcessedFile> failures = appendFailureEntries(errorReportPath, accountCustomerMap);
processedFiles.addAll(failures);
