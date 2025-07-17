private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> errorMap,
        KafkaMessage msg) throws IOException {

    List<String> folders = List.of("email", "archive", "mobstat", "print");
    Map<String, String> folderToOutputMethod = Map.of(
            "email", "EMAIL",
            "archive", "ARCHIVE",
            "mobstat", "MOBSTAT",
            "print", "PRINT"
    );

    List<SummaryProcessedFile> resultList = new ArrayList<>();

    for (SummaryProcessedFile spf : customerList) {
        String account = spf.getAccountNumber();
        String customer = spf.getCustomerNumber();
        if (account == null || account.isBlank()) continue;

        SummaryProcessedFile updatedSpf = new SummaryProcessedFile();
        BeanUtils.copyProperties(spf, updatedSpf); // Ensure we work on a copy

        for (String folder : folders) {
            Path folderPath = jobDir.resolve(folder);
            Optional<Path> fileOpt = Files.exists(folderPath)
                    ? Files.list(folderPath)
                    .filter(p -> p.getFileName().toString().contains(account))
                    .findFirst()
                    : Optional.empty();

            String outputMethod = folderToOutputMethod.get(folder);
            Map<String, String> errorEntry = errorMap.getOrDefault(account, Collections.emptyMap());
            String failureStatus = errorEntry.getOrDefault(outputMethod, "");

            if (fileOpt.isPresent()) {
                Path file = fileOpt.get();
                String blobUrl = blobStorageService.uploadFile(
                        file.toFile(),
                        msg.getSourceSystem() + "/" + msg.getBatchId() + "/" + folder + "/" + file.getFileName()
                );
                String decoded = decodeUrl(blobUrl);

                switch (folder) {
                    case "email" -> {
                        updatedSpf.setPdfEmailFileUrl(decoded);
                        updatedSpf.setPdfEmailStatus("OK");
                    }
                    case "archive" -> {
                        updatedSpf.setPdfArchiveFileUrl(decoded);
                        updatedSpf.setPdfArchiveStatus("OK");
                    }
                    case "mobstat" -> {
                        updatedSpf.setPdfMobstatFileUrl(decoded);
                        updatedSpf.setPdfMobstatStatus("OK");
                    }
                    case "print" -> updatedSpf.setPrintFileUrl(decoded);
                }
            } else {
                boolean isExplicitlyFailed = "Failed".equalsIgnoreCase(failureStatus);
                if (isExplicitlyFailed) {
                    switch (folder) {
                        case "email" -> updatedSpf.setPdfEmailStatus("Failed");
                        case "archive" -> updatedSpf.setPdfArchiveStatus("Failed");
                        case "mobstat" -> updatedSpf.setPdfMobstatStatus("Failed");
                    }
                } else {
                    switch (folder) {
                        case "email" -> updatedSpf.setPdfEmailStatus("");
                        case "archive" -> updatedSpf.setPdfArchiveStatus("");
                        case "mobstat" -> updatedSpf.setPdfMobstatStatus("");
                    }
                }
            }
        }

        List<String> statuses = Arrays.asList(
                updatedSpf.getPdfEmailStatus(),
                updatedSpf.getPdfArchiveStatus(),
                updatedSpf.getPdfMobstatStatus()
        );

        long failedCount = statuses.stream().filter("Failed"::equalsIgnoreCase).count();
        long knownCount = statuses.stream().filter(s -> s != null && !s.isBlank()).count();

        if (failedCount == knownCount && knownCount > 0) {
            updatedSpf.setStatusCode("FAILED");
            updatedSpf.setStatusDescription("All methods failed");
        } else if (failedCount > 0) {
            updatedSpf.setStatusCode("PARTIAL");
            updatedSpf.setStatusDescription("Some methods failed");
        } else {
            updatedSpf.setStatusCode("SUCCESS");
            updatedSpf.setStatusDescription("Success");
        }

        resultList.add(updatedSpf);
    }

    return resultList;
}
