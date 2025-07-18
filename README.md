Map<String, ProcessedFileEntry> outputMap = new LinkedHashMap<>();

for (SummaryProcessedFile spf : processedList) {
    String customer = spf.getCustomerId();
    String account = spf.getAccountNumber();
    String key = customer + "::" + account;

    ProcessedFileEntry entry = outputMap.computeIfAbsent(key, k -> {
        ProcessedFileEntry e = new ProcessedFileEntry();
        e.setCustomerId(customer);
        e.setAccountNumber(account);
        return e;
    });

    String url = spf.getBlobFileURL();
    if (url == null) continue;

    if (url.contains("/archive/")) {
        entry.setPdfArchiveFileUrl(url);
        entry.setPdfArchiveFileUrlStatus(spf.getStatus());
    } else if (url.contains("/email/")) {
        entry.setPdfEmailFileUrl(url);
        entry.setPdfEmailFileUrlStatus(spf.getStatus());
    } else if (url.contains("/print/")) {
        entry.setPrintFileUrl(url);
        entry.setPrintFileUrlStatus(spf.getStatus());
    } else if (url.contains("/mobstat/")) {
        entry.setPdfMobstatFileUrl(url);
        entry.setPdfMobstatFileUrlStatus(spf.getStatus());
    }

    // Optionally set overall status
    entry.setStatusCode(spf.getStatus());
    entry.setStatusDescription(spf.getStatus().equalsIgnoreCase("SUCCESS") ? "Processed Successfully" : "Failed");
}

List<ProcessedFileEntry> processedFileEntries = new ArrayList<>(outputMap.values());
payload.setPayload(processedFileEntries);
