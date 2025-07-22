private static List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path archivePath,
        Map<String, Path> deliveryFolderMap,
        MessageMetaData msg,
        Map<String, Map<String, String>> errorMap
) throws IOException {

    List<SummaryProcessedFile> detailedList = new ArrayList<>();

    for (Map.Entry<String, Path> deliveryEntry : deliveryFolderMap.entrySet()) {
        String outputMethod = deliveryEntry.getKey(); // EMAIL, PRINT, MOBSTAT
        Path folderPath = deliveryEntry.getValue();

        if (!Files.exists(folderPath)) continue;

        Files.list(folderPath)
                .filter(Files::isRegularFile)
                .forEach(filePath -> {
                    try {
                        String fileName = filePath.getFileName().toString();
                        String customerId = extractCustomerId(fileName);
                        String accountNumber = extractAccountNumber(fileName);

                        // Upload output file
                        String outputBlobUrl = blobStorageService.uploadFileByMessage(filePath.toFile(), outputMethod.toLowerCase(), msg);
                        log.info("Uploaded {} file for customerId={}, accountNumber={}: {}", outputMethod, customerId, accountNumber, outputBlobUrl);

                        // Now match corresponding archive file (try prefix/suffix match)
                        Optional<Path> archiveFile = Files.list(archivePath)
                                .filter(Files::isRegularFile)
                                .filter(p -> {
                                    String name = p.getFileName().toString();
                                    return name.contains(accountNumber) || name.startsWith(accountNumber) || name.endsWith(accountNumber + ".pdf");
                                })
                                .findFirst();

                        String archiveBlobUrl = null;
                        String archiveStatus = "FAILED";

                        if (archiveFile.isPresent()) {
                            archiveBlobUrl = blobStorageService.uploadFileByMessage(
                                    archiveFile.get().toFile(), "archive", msg);
                            archiveStatus = "SUCCESS";
                            log.info("Uploaded archive file for customerId={}, accountNumber={}: {}", customerId, accountNumber, archiveBlobUrl);
                        } else {
                            log.warn("Archive file NOT FOUND for customerId={}, accountNumber={} in {}", customerId, accountNumber, archivePath);
                        }

                        // Error status
                        String outputStatus = determineStatus(customerId, accountNumber, outputMethod, errorMap);

                        SummaryProcessedFile entry = new SummaryProcessedFile();
                        entry.setCustomerId(customerId);
                        entry.setAccountNumber(accountNumber);
                        entry.setOutputType(outputMethod);
                        entry.setBlobUrl(outputBlobUrl);
                        entry.setStatus(outputStatus);

                        entry.setArchiveBlobUrl(archiveBlobUrl);
                        entry.setArchiveStatus(archiveStatus);

                        detailedList.add(entry);

                    } catch (Exception e) {
                        log.error("Error while processing file in " + outputMethod + ": " + filePath, e);
                    }
                });
    }

    return detailedList;
}

=============

private static String determineStatus(String customerId, String accountNumber, String outputType,
                                      Map<String, Map<String, String>> errorMap) {
    Map<String, String> accountMap = errorMap.getOrDefault(customerId, Collections.emptyMap());
    String status = accountMap.getOrDefault(accountNumber, "SUCCESS");

    if ("FAILED".equalsIgnoreCase(status)) {
        log.warn("ErrorMap indicates FAILURE for customerId={}, accountNumber={}, output={}", customerId, accountNumber, outputType);
        return "FAILED";
    }
    return "SUCCESS";
}
