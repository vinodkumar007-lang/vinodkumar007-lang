 private List<SummaryProcessedFile> appendFailureEntries(String errorReportFilePath, List<SummaryProcessedFile> successList) {
        List<SummaryProcessedFile> failures = new ArrayList<>();
        if (errorReportFilePath == null) return failures;
        Path path = Paths.get(errorReportFilePath);
        if (!Files.exists(path)) return failures;

        Set<String> successAccounts = new HashSet<>();
        for (SummaryProcessedFile spf : successList) {
            successAccounts.add(spf.getAccountNumber());
        }

        try (BufferedReader reader = Files.newBufferedReader(path)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\|");
                if (parts.length >= 5) {
                    String account = parts[0].trim();
                    String customer = parts[1].trim();
                    String outputMethod = parts[2].trim().toUpperCase();
                    String status = parts[4].trim();

                    if (!successAccounts.contains(account) && "FAILED".equalsIgnoreCase(status)) {
                        SummaryProcessedFile failEntry = new SummaryProcessedFile();
                        failEntry.setAccountNumber(account);
                        failEntry.setCustomerId(customer);
                        failEntry.setStatusCode("FAILURE");
                        failEntry.setStatusDescription("Processing failed");

                        switch (outputMethod) {
                            case "EMAIL"   -> failEntry.setPdfEmailFileUrl("FAILED");
                            case "HTML"    -> failEntry.setHtmlEmailFileUrl("FAILED");
                            case "MOBSTAT" -> failEntry.setPdfMobstatFileUrl("FAILED");
                            case "TXT"     -> failEntry.setTxtEmailFileUrl("FAILED");
                            case "ARCHIVE" -> failEntry.setPdfArchiveFileUrl("FAILED");
                            default         -> logger.warn("⚠️ Unknown OutputMethod in ErrorReport: {}", outputMethod);
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
