private List<CustomerSummary> buildGroupedProcessedFiles(
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

    // Key: customerId -> CustomerSummary
    Map<String, CustomerSummary> customerSummaryMap = new LinkedHashMap<>();

    for (SummaryProcessedFile spf : customerList) {
        String account = spf.getAccountNumber();
        String customer = spf.getCustomerId();
        if (account == null || account.isBlank()) continue;

        // Prepare or get CustomerSummary for this customer
        CustomerSummary customerSummary = customerSummaryMap.computeIfAbsent(customer, c -> {
            CustomerSummary cs = new CustomerSummary();
            cs.setCustomerId(c);
            return cs;
        });

        // Check if this account already processed inside customer's accounts
        boolean alreadyProcessed = customerSummary.getAccounts().stream()
            .anyMatch(a -> a.getAccountNumber().equals(account));
        if (alreadyProcessed) continue; // avoid duplicate accounts

        // Prepare a SummaryProcessedFile entry to accumulate statuses & URLs
        SummaryProcessedFile entry = new SummaryProcessedFile();
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
                    logger.warn("Could not scan folder '{}': {}", folder, e.getMessage());
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
                // File not found
                boolean isExplicitFail = "Failed".equalsIgnoreCase(failureStatus);
                switch (folder) {
                    case "email" -> entry.setPdfEmailStatus(isExplicitFail ? "Failed" : "Skipped");
                    case "archive" -> entry.setPdfArchiveStatus(isExplicitFail ? "Failed" : "Skipped");
                    case "mobstat" -> entry.setPdfMobstatStatus(isExplicitFail ? "Failed" : "Skipped");
                    case "print" -> entry.setPrintStatus(isExplicitFail ? "Failed" : "Skipped");
                }
            }
        }

        // Determine final status per account entry
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
            entry.setStatusDescription("Some methods failed or skipped");
        } else if (failed > 0 && success == 0) {
            entry.setStatusCode("FAILED");
            entry.setStatusDescription("All methods failed");
        } else if (success == 0 && skipped > 0) {
            entry.setStatusCode("SKIPPED");
            entry.setStatusDescription("Files not found");
        } else {
            entry.setStatusCode("SUCCESS");
            entry.setStatusDescription("Success");
        }

        customerSummary.getAccounts().add(entry);
    }

    // Also handle accounts in errorMap but missing in input
    for (String account : errorMap.keySet()) {
        // Check if account is already included
        boolean exists = customerSummaryMap.values().stream()
                .flatMap(cs -> cs.getAccounts().stream())
                .anyMatch(a -> a.getAccountNumber().equals(account));

        if (!exists) {
            SummaryProcessedFile err = new SummaryProcessedFile();
            err.setAccountNumber(account);
            err.setCustomerId("UNKNOWN"); // fallback

            Map<String, String> methodStatusMap = errorMap.get(account);
            if (methodStatusMap != null) {
                if ("Failed".equalsIgnoreCase(methodStatusMap.get("EMAIL"))) {
                    err.setPdfEmailStatus("Failed");
                }
                if ("Failed".equalsIgnoreCase(methodStatusMap.get("ARCHIVE"))) {
                    err.setPdfArchiveStatus("Failed");
                }
                if ("Failed".equalsIgnoreCase(methodStatusMap.get("MOBSTAT"))) {
                    err.setPdfMobstatStatus("Failed");
                }
                if ("Failed".equalsIgnoreCase(methodStatusMap.get("PRINT"))) {
                    err.setPrintStatus("Failed");
                }
            }

            err.setStatusCode("FAILED");
            err.setStatusDescription("Failed due to error report");

            // Create or get dummy customer summary for UNKNOWN
            CustomerSummary unknownCustomer = customerSummaryMap.computeIfAbsent("UNKNOWN", c -> {
                CustomerSummary cs = new CustomerSummary();
                cs.setCustomerId(c);
                return cs;
            });
            unknownCustomer.getAccounts().add(err);
        }
    }

    return new ArrayList<>(customerSummaryMap.values());
}



public class CustomerSummary {
    private String customerId;
    private List<SummaryProcessedFile> accounts = new ArrayList<>();

    // getters and setters
}
