private List<SummaryProcessedFile> buildDetailedProcessedFiles(
    Path jobDir,
    Map<String, String> errorMap,
    KafkaMessage message
) {
    List<SummaryProcessedFile> mergedList = new ArrayList<>();
    Map<String, SummaryProcessedFile> customerMap = new HashMap<>();

    List<String> folders = List.of("archive", "email", "html", "mobstat", "print");

    for (String folder : folders) {
        Path folderPath = jobDir.resolve(folder);
        if (!Files.exists(folderPath)) continue;

        try (Stream<Path> files = Files.list(folderPath)) {
            files.filter(Files::isRegularFile).forEach(file -> {
                try {
                    String fileName = file.getFileName().toString();

                    if (fileName.equalsIgnoreCase("mobstat_trigger.txt")) {
                        // skip this file
                        return;
                    }

                    // Extract account number and customer number from filename
                    String[] parts = fileName.split("_");
                    if (parts.length < 2) return;

                    String accountNumber = parts[0];
                    String customerNumber = parts[1].replaceAll("\\D", ""); // remove extension or symbols

                    String key = accountNumber + "_" + customerNumber;

                    SummaryProcessedFile processedFile = customerMap.getOrDefault(key, new SummaryProcessedFile());
                    processedFile.setAccountNumber(accountNumber);
                    processedFile.setCustomerNumber(customerNumber);

                    // Get blob URL
                    String blobUrl = "https://" + storageAccount + ".blob.core.windows.net/" +
                            containerName + "/" + message.getJobId() + "/" + folder + "/" + fileName;

                    // Set status: default is "SUCCESS"
                    String status = "SUCCESS";
                    String errorKey = key + "_" + folder;

                    if (!Files.exists(file)) {
                        if (errorMap.containsKey(errorKey)) {
                            status = "FAILED";
                        } else {
                            status = "";
                        }
                    }

                    // Add URL to the corresponding list
                    switch (folder.toLowerCase()) {
                        case "archive" -> {
                            processedFile.setArchiveURL(blobUrl);
                            processedFile.setArchiveStatus(status);
                        }
                        case "email" -> {
                            processedFile.setEmailURL(blobUrl);
                            processedFile.setEmailStatus(status);
                        }
                        case "html" -> {
                            processedFile.setHtmlURL(blobUrl);
                            processedFile.setHtmlStatus(status);
                        }
                        case "mobstat" -> {
                            processedFile.setMobstatURL(blobUrl);
                            processedFile.setMobstatStatus(status);
                        }
                        case "print" -> {
                            processedFile.setPrintURL(blobUrl);
                            processedFile.setPrintStatus(status);
                        }
                    }

                    customerMap.put(key, processedFile);

                } catch (Exception e) {
                    logger.error("❌ Error processing file in folder {}: {}", folder, e.getMessage());
                }
            });
        } catch (IOException e) {
            logger.error("❌ Failed to read folder {}: {}", folder, e.getMessage());
        }
    }

    mergedList.addAll(customerMap.values());
    return mergedList;
}
