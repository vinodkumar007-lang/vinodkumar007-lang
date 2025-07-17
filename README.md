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

    Map<String, SummaryProcessedFile> outputMap = new LinkedHashMap<>();

    for (SummaryProcessedFile spf : customerList) {
        String account = spf.getAccountNumber();
        String customer = spf.getCustomerId();
        if (account == null || account.isBlank()) continue;

        String key = customer + "::" + account;
        SummaryProcessedFile entry = outputMap.getOrDefault(key, new SummaryProcessedFile());
        BeanUtils.copyProperties(spf, entry);

        for (String folder : folders) {
            String outputMethod = folderToOutputMethod.get(folder);

            Path folderPath = jobDir.resolve(folder);
            Path matchedFile = null;
            if (Files.exists(folderPath)) {
                try (Stream<Path> files = Files.list(folderPath)) {
                    matchedFile = files
                            .filter(p -> p.getFileName().toString().contains(account))
                            .filter(p -> !(folder.equals("mobstat") && p.getFileName().toString().toLowerCase().contains("trigger")))
                            .findFirst()
                            .orElse(null);
                } catch (IOException e) {
                    logger.warn("⚠️ Could not scan folder '{}': {}", folder, e.getMessage());
                }
            }

            String failureStatus = errorMap.getOrDefault(account, Collections.emptyMap()).getOrDefault(outputMethod, "");

            if (matchedFile != null) {
                String blobUrl = blobStorageService.uploadFile(
                        matchedFile.toFile(),
                        msg.getSourceSystem() + "/" + msg.getBatchId() + "/" + folder + "/" + matchedFile.getFileName()
                );
                String decoded = decodeUrl(blobUrl);

                switch (folder) {
                    case "email" -> {
                        entry.setPdfEmailFileUrl(decoded);
                        entry.setPdfEmailStatus("OK");
                    }
                    case "archive" -> {
                        entry.setPdfArchiveFileUrl(decoded);
                        entry.setPdfArchiveStatus("OK");
                    }
                    case "mobstat" -> {
                        entry.setPdfMobstatFileUrl(decoded);
                        entry.setPdfMobstatStatus("OK");
                    }
                    case "print" -> {
                        entry.setPrintFileUrl(decoded);
                        entry.setPrintStatus("OK");
                    }
                }
            } else {
                boolean isExplicitFail = "Failed".equalsIgnoreCase(failureStatus);
                switch (folder) {
                    case "email" -> entry.setPdfEmailStatus(isExplicitFail ? "Failed" : "Skipped");
                    case "archive" -> entry.setPdfArchiveStatus(isExplicitFail ? "Failed" : "Skipped");
                    case "mobstat" -> entry.setPdfMobstatStatus(isExplicitFail ? "Failed" : "Skipped");
                    case "print" -> entry.setPrintStatus(isExplicitFail ? "Failed" : "Skipped");
                }
            }
        }

        // ✅ Determine overall status
        List<String> statuses = Arrays.asList(
                entry.getPdfEmailStatus(),
                entry.getPdfArchiveStatus(),
                entry.getPdfMobstatStatus(),
                entry.getPrintStatus()
        );

        long failed = statuses.stream().filter("Failed"::equalsIgnoreCase).count();
        long skipped = statuses.stream().filter("Skipped"::equalsIgnoreCase).count();
        long success = statuses.stream().filter("OK"::equalsIgnoreCase).count();

        if (success > 0 && (failed > 0 || skipped > 0)) {
            entry.setStatusCode("PARTIAL");
            entry.setStatusDescription("Some outputs failed or skipped");
        } else if (failed > 0 && success == 0) {
            entry.setStatusCode("FAILED");
            entry.setStatusDescription("All outputs failed");
        } else if (success == 0 && skipped > 0) {
            entry.setStatusCode("SKIPPED");
            entry.setStatusDescription("No files found");
        } else {
            entry.setStatusCode("SUCCESS");
            entry.setStatusDescription("All outputs successful");
        }

        outputMap.put(key, entry);
    }

    // ✅ Include additional failures from errorMap not in customerList
    for (String account : errorMap.keySet()) {
        for (String outputMethod : errorMap.get(account).keySet()) {
            boolean exists = outputMap.values().stream()
                    .anyMatch(e -> account.equals(e.getAccountNumber()));
            if (!exists) {
                SummaryProcessedFile err = new SummaryProcessedFile();
                err.setAccountNumber(account);
                err.setCustomerId("UNKNOWN");

                switch (outputMethod) {
                    case "EMAIL" -> err.setPdfEmailStatus("Failed");
                    case "ARCHIVE" -> err.setPdfArchiveStatus("Failed");
                    case "MOBSTAT" -> err.setPdfMobstatStatus("Failed");
                    case "PRINT" -> err.setPrintStatus("Failed");
                }

                err.setStatusCode("FAILED");
                err.setStatusDescription("Failed due to error report");
                outputMap.put("error::" + account, err);
            }
        }
    }

    return new ArrayList<>(outputMap.values());
}
