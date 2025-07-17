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

    Map<String, SummaryProcessedFile> groupedMap = new LinkedHashMap<>();

    for (SummaryProcessedFile spf : customerList) {
        String account = spf.getAccountNumber();
        String customer = spf.getCustomerId();
        if (account == null || account.isBlank()) continue;

        String key = account + "::" + customer;
        SummaryProcessedFile existing = groupedMap.computeIfAbsent(key, k -> {
            SummaryProcessedFile newSpf = new SummaryProcessedFile();
            BeanUtils.copyProperties(spf, newSpf);
            return newSpf;
        });

        for (String folder : folders) {
            Path folderPath = jobDir.resolve(folder);
            Optional<Path> fileOpt;

            if (!Files.exists(folderPath)) continue;

            Stream<Path> fileStream = Files.list(folderPath)
                    .filter(p -> p.getFileName().toString().contains(account));

            // For mobstat, exclude trigger files from status check
            if (folder.equals("mobstat")) {
                fileStream = fileStream.filter(p -> !p.getFileName().toString().toLowerCase().endsWith("_trigger.triggr"));
            }

            List<Path> matchingFiles = fileStream.collect(Collectors.toList());

            String outputMethod = folderToOutputMethod.get(folder);
            String failureStatus = errorMap.getOrDefault(account, Collections.emptyMap())
                    .getOrDefault(outputMethod, "");

            if (!matchingFiles.isEmpty()) {
                // Upload first valid file (can be extended to all)
                Path file = matchingFiles.get(0);
                String blobUrl = blobStorageService.uploadFile(
                        file.toFile(),
                        msg.getSourceSystem() + "/" + msg.getBatchId() + "/" + folder + "/" + file.getFileName()
                );
                String decoded = decodeUrl(blobUrl);

                switch (folder) {
                    case "email" -> {
                        existing.setPdfEmailFileUrl(decoded);
                        existing.setPdfEmailStatus("OK");
                    }
                    case "archive" -> {
                        existing.setPdfArchiveFileUrl(decoded);
                        existing.setPdfArchiveStatus("OK");
                    }
                    case "mobstat" -> {
                        existing.setPdfMobstatFileUrl(decoded);
                        existing.setPdfMobstatStatus("OK");
                    }
                    case "print" -> {
                        existing.setPrintFileUrl(decoded);
                        existing.setPrintStatus("OK");
                    }
                }
            } else {
                boolean isExplicitFail = "Failed".equalsIgnoreCase(failureStatus);
                switch (folder) {
                    case "email" -> existing.setPdfEmailStatus(isExplicitFail ? "Failed" : "");
                    case "archive" -> existing.setPdfArchiveStatus(isExplicitFail ? "Failed" : "");
                    case "mobstat" -> existing.setPdfMobstatStatus(isExplicitFail ? "Failed" : "");
                    case "print" -> existing.setPrintStatus(isExplicitFail ? "Failed" : "");
                }
            }
        }

        // Skip customers with no delivery
        List<String> statuses = Arrays.asList(
                existing.getPdfEmailStatus(),
                existing.getPdfArchiveStatus(),
                existing.getPdfMobstatStatus(),
                existing.getPrintStatus()
        );

        boolean noDelivery = statuses.stream().allMatch(s -> s == null || s.isBlank());
        if (noDelivery) {
            groupedMap.remove(key);
            continue;
        }

        // Final status code
        long failed = statuses.stream().filter("Failed"::equalsIgnoreCase).count();
        long success = statuses.stream().filter("OK"::equalsIgnoreCase).count();

        if (failed > 0 && success == 0) {
            existing.setStatusCode("FAILED");
            existing.setStatusDescription("All methods failed");
        } else if (failed > 0) {
            existing.setStatusCode("PARTIAL");
            existing.setStatusDescription("Some methods failed");
        } else {
            existing.setStatusCode("SUCCESS");
            existing.setStatusDescription("Success");
        }
    }

    return new ArrayList<>(groupedMap.values());
}
