private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        Map<String, SummaryProcessedFile> customerMap,
        Map<String, Map<String, String>> errorMap,
        KafkaMessage msg) throws IOException {

    List<String> folders = List.of("email", "archive", "mobstat", "print");
    Map<String, String> folderToOutputMethod = Map.of(
            "email", "EMAIL",
            "archive", "ARCHIVE",
            "mobstat", "MOBSTAT",
            "print", "PRINT"
    );

    Map<String, SummaryProcessedFile> resultMap = new HashMap<>();

    for (Map.Entry<String, SummaryProcessedFile> entry : customerMap.entrySet()) {
        String account = entry.getKey();
        if (account == null || account.isBlank()) continue;

        SummaryProcessedFile spf = entry.getValue();
        Map<String, Boolean> methodFound = new HashMap<>();

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

            boolean fileFound = fileOpt.isPresent();
            methodFound.put(outputMethod, fileFound);

            if (fileFound) {
                Path file = fileOpt.get();
                String blobUrl = blobStorageService.uploadFile(
                        file.toFile(),
                        msg.getSourceSystem() + "/" + msg.getBatchId() + "/" + folder + "/" + file.getFileName()
                );
                String decoded = decodeUrl(blobUrl);

                switch (folder) {
                    case "email" -> {
                        spf.setPdfEmailFileUrl(decoded);
                        spf.setPdfEmailStatus("OK");
                    }
                    case "archive" -> {
                        spf.setPdfArchiveFileUrl(decoded);
                        spf.setPdfArchiveStatus("OK");
                    }
                    case "mobstat" -> {
                        spf.setPdfMobstatFileUrl(decoded);
                        spf.setPdfMobstatStatus("OK");
                    }
                    case "print" -> spf.setPrintFileUrl(decoded);
                }
            } else {
                boolean isExplicitlyFailed = "Failed".equalsIgnoreCase(failureStatus);
                if (isExplicitlyFailed) {
                    switch (folder) {
                        case "email" -> spf.setPdfEmailStatus("Failed");
                        case "archive" -> spf.setPdfArchiveStatus("Failed");
                        case "mobstat" -> spf.setPdfMobstatStatus("Failed");
                    }
                } else {
                    switch (folder) {
                        case "email" -> spf.setPdfEmailStatus("");
                        case "archive" -> spf.setPdfArchiveStatus("");
                        case "mobstat" -> spf.setPdfMobstatStatus("");
                    }
                }
            }
        }

        List<String> statuses = Arrays.asList(
                spf.getPdfEmailStatus(),
                spf.getPdfArchiveStatus(),
                spf.getPdfMobstatStatus()
        );

        long failedCount = statuses.stream().filter("Failed"::equalsIgnoreCase).count();
        long knownCount = statuses.stream().filter(s -> s != null && !s.isBlank()).count();

        if (failedCount == knownCount && knownCount > 0) {
            spf.setStatusCode("FAILED");
            spf.setStatusDescription("All methods failed");
        } else if (failedCount > 0) {
            spf.setStatusCode("PARTIAL");
            spf.setStatusDescription("Some methods failed");
        } else {
            spf.setStatusCode("SUCCESS");
            spf.setStatusDescription("Success");
        }

        // 🔁 Use merging logic if duplicate accountNumber already exists
        resultMap.merge(
                account,
                spf,
                (existing, incoming) -> mergeFiles(existing, incoming)
        );
    }

    return new ArrayList<>(resultMap.values());
}

private SummaryProcessedFile mergeFiles(SummaryProcessedFile f1, SummaryProcessedFile f2) {
    SummaryProcessedFile result = new SummaryProcessedFile();
    result.setAccountNumber(f1.getAccountNumber());

    result.setCustomerId(firstNonNull(f1.getCustomerId(), f2.getCustomerId()));
    result.setFirstName(firstNonNull(f1.getFirstName(), f2.getFirstName()));
    result.setLastName(firstNonNull(f1.getLastName(), f2.getLastName()));
    result.setEmail(firstNonNull(f1.getEmail(), f2.getEmail()));
    result.setMobileNumber(firstNonNull(f1.getMobileNumber(), f2.getMobileNumber()));
    result.setAddressLine1(firstNonNull(f1.getAddressLine1(), f2.getAddressLine1()));
    result.setAddressLine2(firstNonNull(f1.getAddressLine2(), f2.getAddressLine2()));
    result.setAddressLine3(firstNonNull(f1.getAddressLine3(), f2.getAddressLine3()));
    result.setPostalCode(firstNonNull(f1.getPostalCode(), f2.getPostalCode()));
    result.setContactNumber(firstNonNull(f1.getContactNumber(), f2.getContactNumber()));
    result.setProduct(firstNonNull(f1.getProduct(), f2.getProduct()));
    result.setTemplateCode(firstNonNull(f1.getTemplateCode(), f2.getTemplateCode()));
    result.setTemplateName(firstNonNull(f1.getTemplateName(), f2.getTemplateName()));
    result.setBalance(firstNonNull(f1.getBalance(), f2.getBalance()));
    result.setCreditLimit(firstNonNull(f1.getCreditLimit(), f2.getCreditLimit()));
    result.setInterestRate(firstNonNull(f1.getInterestRate(), f2.getInterestRate()));
    result.setDueAmount(firstNonNull(f1.getDueAmount(), f2.getDueAmount()));
    result.setArrears(firstNonNull(f1.getArrears(), f2.getArrears()));
    result.setDueDate(firstNonNull(f1.getDueDate(), f2.getDueDate()));
    result.setIdNumber(firstNonNull(f1.getIdNumber(), f2.getIdNumber()));
    result.setAccountReference(firstNonNull(f1.getAccountReference(), f2.getAccountReference()));
    result.setPdfArchiveFileUrl(firstNonNull(f1.getPdfArchiveFileUrl(), f2.getPdfArchiveFileUrl()));
    result.setPdfArchiveStatus(firstNonNull(f1.getPdfArchiveStatus(), f2.getPdfArchiveStatus()));
    result.setPdfEmailFileUrl(firstNonNull(f1.getPdfEmailFileUrl(), f2.getPdfEmailFileUrl()));
    result.setPdfEmailStatus(firstNonNull(f1.getPdfEmailStatus(), f2.getPdfEmailStatus()));
    result.setPdfMobstatFileUrl(firstNonNull(f1.getPdfMobstatFileUrl(), f2.getPdfMobstatFileUrl()));
    result.setPdfMobstatStatus(firstNonNull(f1.getPdfMobstatStatus(), f2.getPdfMobstatStatus()));
    result.setPrintFileUrl(firstNonNull(f1.getPrintFileUrl(), f2.getPrintFileUrl()));
    result.setPrintStatus(firstNonNull(f1.getPrintStatus(), f2.getPrintStatus()));
    result.setStatusCode(firstNonNull(f1.getStatusCode(), f2.getStatusCode()));
    result.setStatusDescription(firstNonNull(f1.getStatusDescription(), f2.getStatusDescription()));
    result.setFullName(firstNonNull(f1.getFullName(), f2.getFullName()));
    result.setBlobURL(firstNonNull(f1.getBlobURL(), f2.getBlobURL()));

    return result;
}

private <T> T firstNonNull(T val1, T val2) {
    return val1 != null ? val1 : val2;
}
