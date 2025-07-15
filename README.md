private List<SummaryProcessedFile> buildAndUploadProcessedFiles(
    Path jobDir,
    Map<String, String> accountCustomerMap,
    KafkaMessage msg,
    Set<String> errorReportEntries
) throws IOException {

    List<SummaryProcessedFile> result = new ArrayList<>();
    List<String> folders = List.of("archive", "email", "html", "mobstat", "txt");

    for (Map.Entry<String, String> entry : accountCustomerMap.entrySet()) {
        String accountNumber = entry.getKey();
        String customerId = entry.getValue();

        SummaryProcessedFile summary = new SummaryProcessedFile();
        summary.setCustomerId(customerId);
        summary.setAccountNumber(accountNumber);

        Map<String, Boolean> folderPresence = new HashMap<>();

        for (String folder : folders) {
            String filePrefix = accountNumber + "_";
            Path folderPath = jobDir.resolve(folder);

            Optional<Path> matchedFile = Files.exists(folderPath)
                ? Files.list(folderPath)
                        .filter(Files::isRegularFile)
                        .filter(f -> f.getFileName().toString().startsWith(filePrefix))
                        .findFirst()
                : Optional.empty();

            if (matchedFile.isPresent()) {
                Path file = matchedFile.get();
                String url = blobStorageService.uploadBinaryFile(file.toFile(), msg, folder);

                switch (folder) {
                    case "archive" -> {
                        summary.setPdfArchiveFileUrl(url);
                        summary.setPdfArchiveStatus("OK");
                    }
                    case "email" -> {
                        summary.setPdfEmailFileUrl(url);
                        summary.setPdfEmailStatus("OK");
                    }
                    case "html" -> {
                        summary.setPdfHtmlFileUrl(url);
                        summary.setPdfHtmlStatus("OK");
                    }
                    case "mobstat" -> {
                        summary.setPdfMobstatFileUrl(url);
                        summary.setPdfMobstatStatus("OK");
                    }
                    case "txt" -> {
                        summary.setPdfTxtFileUrl(url);
                        summary.setPdfTxtStatus("OK");
                    }
                }

                folderPresence.put(folder, true);
            } else {
                String errorKey = accountNumber + "|" + folder;
                boolean markedFailed = errorReportEntries.contains(errorKey);

                switch (folder) {
                    case "archive" -> summary.setPdfArchiveStatus(markedFailed ? "FAILED" : null);
                    case "email" -> summary.setPdfEmailStatus(markedFailed ? "FAILED" : null);
                    case "html" -> summary.setPdfHtmlStatus(markedFailed ? "FAILED" : null);
                    case "mobstat" -> summary.setPdfMobstatStatus(markedFailed ? "FAILED" : null);
                    case "txt" -> summary.setPdfTxtStatus(markedFailed ? "FAILED" : null);
                }

                folderPresence.put(folder, false);
            }
        }

        long total = folders.size();
        long found = folderPresence.values().stream().filter(Boolean::booleanValue).count();

        if (found == total) {
            summary.setStatusCode("OK");
            summary.setStatusDescription("All files present");
        } else if (found == 0) {
            summary.setStatusCode("FAILED");
            summary.setStatusDescription("No files found");
        } else {
            summary.setStatusCode("PARTIAL");
            summary.setStatusDescription("Some files missing");
        }

        result.add(summary);
    }

    return result;
}
