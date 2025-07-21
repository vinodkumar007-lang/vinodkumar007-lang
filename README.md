private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> errorMap,
        KafkaMessage msg) throws IOException {

    List<String> folders = List.of("email", "mobstat", "print");
    Map<String, String> folderToOutputMethod = Map.of(
            "email", "EMAIL",
            "mobstat", "MOBSTAT",
            "print", "PRINT"
    );

    List<SummaryProcessedFile> finalList = new ArrayList<>();

    for (SummaryProcessedFile customer : customerList) {
        String account = customer.getAccountNumber();
        String customerId = customer.getCustomerId();
        boolean hasAtLeastOneSuccess = false;

        // Track which output methods have a match
        Map<String, Boolean> methodAdded = new HashMap<>();

        for (String folder : folders) {
            methodAdded.put(folder, false);
            Path folderPath = jobDir.resolve(folder);
            if (!Files.exists(folderPath)) continue;

            Optional<Path> match = Files.list(folderPath)
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().contains(account))
                    .findFirst();

            if (match.isPresent()) {
                Path filePath = match.get();
                File file = filePath.toFile();
                String blobUrl = blobStorageService.uploadFile(file, folder, msg);

                SummaryProcessedFile successEntry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, successEntry);
                successEntry.setOutputMethod(folderToOutputMethod.get(folder));
                successEntry.setStatus("SUCCESS");
                successEntry.setBlobURL(blobUrl);
                finalList.add(successEntry);

                hasAtLeastOneSuccess = true;
                methodAdded.put(folder, true);
            }
        }

        // 🔗 Handle archive files for each delivery method individually
        for (String folder : folders) {
            String deliveryType = folderToOutputMethod.get(folder);
            Path archivePath = jobDir.resolve("archive");
            if (!Files.exists(archivePath)) continue;

            Optional<Path> archiveMatch = Files.list(archivePath)
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().contains(account))
                    .findFirst();

            if (archiveMatch.isPresent()) {
                Path archiveFile = archiveMatch.get();
                File file = archiveFile.toFile();
                String blobUrl = blobStorageService.uploadFile(file, "archive", msg);

                SummaryProcessedFile archiveEntry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, archiveEntry);
                archiveEntry.setOutputMethod("ARCHIVE");
                archiveEntry.setStatus("SUCCESS");
                archiveEntry.setBlobURL(blobUrl);
                archiveEntry.setLinkedDeliveryType(deliveryType); // 🔗 Important line
                finalList.add(archiveEntry);
            } else {
                SummaryProcessedFile archiveFail = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, archiveFail);
                archiveFail.setOutputMethod("ARCHIVE");
                archiveFail.setStatus("FAILED");
                archiveFail.setStatusDescription("Archive not found for delivery: " + deliveryType);
                archiveFail.setReason("Missing archive for " + deliveryType);
                archiveFail.setBlobURL(null);
                archiveFail.setLinkedDeliveryType(deliveryType); // 🔗 Important line
                finalList.add(archiveFail);
            }
        }

        // ⛔ Handle failed delivery methods
        for (String folder : folders) {
            if (methodAdded.get(folder)) continue;

            Path folderPath = jobDir.resolve(folder);
            if (Files.exists(folderPath)) {
                SummaryProcessedFile failedEntry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, failedEntry);
                failedEntry.setOutputMethod(folderToOutputMethod.get(folder));
                failedEntry.setStatus("FAILED");
                failedEntry.setStatusDescription("File not found for method: " + folder);
                failedEntry.setReason("File not found in " + folder + " folder");
                failedEntry.setBlobURL(null);
                finalList.add(failedEntry);
            }
        }

        // ⛔ Error Report Handling
        if (errorMap.containsKey(account)) {
            Map<String, String> errData = errorMap.get(account);
            SummaryProcessedFile errorEntry = new SummaryProcessedFile();
            BeanUtils.copyProperties(customer, errorEntry);
            errorEntry.setOutputMethod("ERROR_REPORT");
            errorEntry.setStatus("FAILED");
            errorEntry.setStatusDescription("Marked as failed from ErrorReport");
            errorEntry.setReason(errData.get("reason"));
            errorEntry.setBlobURL(errData.get("blobURL"));
            finalList.add(errorEntry);
        }
    }

    return finalList;
}

