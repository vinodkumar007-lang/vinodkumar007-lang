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
        BeanUtils.copyProperties(spf, updatedSpf);

        int successCount = 0;

        for (String folder : folders) {
            Path folderPath = jobDir.resolve(folder);
            Optional<Path> fileOpt;

            if (folder.equals("mobstat")) {
                fileOpt = Files.exists(folderPath)
                        ? Files.list(folderPath)
                        .filter(p -> p.getFileName().toString().toLowerCase().contains("mobstat_trigger") &&
                                     p.getFileName().toString().contains(account))
                        .findFirst()
                        : Optional.empty();
            } else {
                fileOpt = Files.exists(folderPath)
                        ? Files.list(folderPath)
                        .filter(p -> p.getFileName().toString().contains(account))
                        .findFirst()
                        : Optional.empty();
            }

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
                    case "print" -> {
                        updatedSpf.setPrintFileUrl(decoded);
                        updatedSpf.setPrintStatus("OK");
                    }
                }
                successCount++;
            } else {
                boolean isExplicitFail = "Failed".equalsIgnoreCase(failureStatus);
                switch (folder) {
                    case "email" -> updatedSpf.setPdfEmailStatus(isExplicitFail ? "Failed" : "");
                    case "archive" -> updatedSpf.setPdfArchiveStatus(isExplicitFail ? "Failed" : "");
                    case "mobstat" -> updatedSpf.setPdfMobstatStatus(isExplicitFail ? "Failed" : "");
                    case "print" -> updatedSpf.setPrintStatus(isExplicitFail ? "Failed" : "");
                }
            }
        }

        // 🔴 Skip adding to resultList if all statuses are empty or all are "Failed"
        List<String> statuses = Arrays.asList(
                updatedSpf.getPdfEmailStatus(),
                updatedSpf.getPdfArchiveStatus(),
                updatedSpf.getPdfMobstatStatus(),
                updatedSpf.getPrintStatus()
        );

        boolean allBlank = statuses.stream().allMatch(s -> s == null || s.isBlank());
        boolean allFailed = statuses.stream().allMatch("Failed"::equalsIgnoreCase);

        if (allBlank || allFailed) {
            continue; // skip this account+customer from final list
        }

        // ✅ Status classification
        long failed = statuses.stream().filter("Failed"::equalsIgnoreCase).count();
        long ok = statuses.stream().filter("OK"::equalsIgnoreCase).count();

        if (failed > 0 && ok == 0) {
            updatedSpf.setStatusCode("FAILED");
            updatedSpf.setStatusDescription("All methods failed");
        } else if (failed > 0) {
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
