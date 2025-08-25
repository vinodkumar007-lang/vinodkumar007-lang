private static List<ProcessedFileEntry> buildProcessedFileEntries(
            List<SummaryProcessedFile> processedFiles,
            Map<String, Map<String, String>> errorMap,
            List<PrintFile> printFiles) {

        List<ProcessedFileEntry> allEntries = new ArrayList<>();

        for (SummaryProcessedFile file : processedFiles) {
            ProcessedFileEntry entry = new ProcessedFileEntry();
            entry.setCustomerId(file.getCustomerId());
            entry.setAccountNumber(file.getAccountNumber());

            String outputType = file.getOutputType();
            String blobUrl = file.getBlobUrl();
            Map<String, String> errors = errorMap.getOrDefault(file.getAccountNumber(), Collections.emptyMap());

            // Determine delivery status
            String status;
            if (isNonEmpty(blobUrl)) {
                status = "SUCCESS";
            } else if ("FAILED".equalsIgnoreCase(errors.getOrDefault(outputType, ""))) {
                status = "FAILED";
            } else {
                status = "";
            }

            switch (outputType) {
                case "EMAIL" -> {
                    entry.setEmailBlobUrl(blobUrl);
                    entry.setEmailStatus(status);
                }
                case "PRINT" -> {
                    if (printFiles != null && !printFiles.isEmpty()) {
                        entry.setPrintStatus("SUCCESS");
                    } else {
                        entry.setPrintStatus("");
                    }
                }
                case "MOBSTAT" -> {
                    entry.setMobstatBlobUrl(blobUrl);
                    entry.setMobstatStatus(status);
                }
                case "ARCHIVE" -> {
                    entry.setArchiveBlobUrl(blobUrl);
                    entry.setArchiveStatus(status);
                }
            }

            // ---- overallStatus per entry (each archive combination gets its own record) ----
            String email = entry.getEmailStatus();
            String print = entry.getPrintStatus();
            String mobstat = entry.getMobstatStatus();
            String archive = entry.getArchiveStatus();

            boolean isEmailSuccess = "SUCCESS".equals(email);
            boolean isPrintSuccess = "SUCCESS".equals(print);
            boolean isMobstatSuccess = "SUCCESS".equals(mobstat);
            boolean isArchiveSuccess = "SUCCESS".equals(archive);

            boolean isEmailMissingOrFailed = email == null || "FAILED".equals(email) || "".equals(email);
            boolean isPrintMissingOrFailed = print == null || "FAILED".equals(print) || "".equals(print);
            boolean isMobstatMissingOrFailed = mobstat == null || "FAILED".equals(mobstat) || "".equals(mobstat);

            if (isEmailSuccess && isArchiveSuccess) {
                entry.setOverallStatus("SUCCESS");
            } else if (isMobstatSuccess && isArchiveSuccess && isEmailMissingOrFailed && isPrintMissingOrFailed) {
                entry.setOverallStatus("SUCCESS");
            } else if (isPrintSuccess && isArchiveSuccess && isEmailMissingOrFailed && isMobstatMissingOrFailed) {
                entry.setOverallStatus("SUCCESS");
            } else if (isArchiveSuccess && isEmailMissingOrFailed && isMobstatMissingOrFailed && isPrintMissingOrFailed) {
                entry.setOverallStatus("PARTIAL");
            } else if (isArchiveSuccess) {
                entry.setOverallStatus("PARTIAL");
            } else {
                entry.setOverallStatus("FAILED");
            }

            if (errorMap.containsKey(entry.getAccountNumber())
                    && !"FAILED".equals(entry.getOverallStatus())) {
                entry.setOverallStatus("PARTIAL");
            }

            allEntries.add(entry);
        }

        return allEntries;
    }
