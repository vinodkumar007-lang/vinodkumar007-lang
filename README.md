private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> inputFiles) {
    Map<String, ProcessedFileEntry> groupedEntries = new LinkedHashMap<>();

    for (SummaryProcessedFile file : inputFiles) {
        String key = file.getCustomerId() + "|" + file.getAccountNumber();

        ProcessedFileEntry entry = groupedEntries.getOrDefault(key, new ProcessedFileEntry());
        entry.setCustomerId(file.getCustomerId());
        entry.setAccountNumber(file.getAccountNumber());

        String outputType = file.getOutputType();
        String blobUrl = file.getBlobUrl();
        String status = file.getStatus();

        switch (outputType) {
            case "EMAIL":
                entry.setPdfEmailFileUrl(blobUrl);
                entry.setPdfEmailFileUrlStatus(status);
                break;
            case "MOBSTAT":
                entry.setPdfMobstatFileUrl(blobUrl);
                entry.setPdfMobstatFileUrlStatus(status);
                break;
            case "PRINT":
                entry.setPrintFileUrl(blobUrl);
                entry.setPrintFileUrlStatus(status);
                break;
            case "ARCHIVE":
                entry.setArchiveBlobUrl(blobUrl);
                entry.setArchiveStatus(status);
                break;
        }

        groupedEntries.put(key, entry);
    }

    // Set overall status after all entries are grouped
    for (ProcessedFileEntry entry : groupedEntries.values()) {
        String emailStatus = entry.getPdfEmailFileUrlStatus();
        String archiveStatus = entry.getArchiveStatus();

        if ("SUCCESS".equalsIgnoreCase(emailStatus) && "SUCCESS".equalsIgnoreCase(archiveStatus)) {
            entry.setOverallStatus("SUCCESS");
        } else if (("SUCCESS".equalsIgnoreCase(emailStatus) && !"SUCCESS".equalsIgnoreCase(archiveStatus)) ||
                   (!"SUCCESS".equalsIgnoreCase(emailStatus) && "SUCCESS".equalsIgnoreCase(archiveStatus))) {
            entry.setOverallStatus("PARTIAL");
        } else {
            entry.setOverallStatus("FAILED");
        }
    }

    return new ArrayList<>(groupedEntries.values());
}

@JsonInclude(JsonInclude.Include.NON_NULL)
@Data
public class ProcessedFileEntry {
    private String customerId;
    private String accountNumber;
    private String pdfEmailFileUrl;
    private String pdfEmailFileUrlStatus;
    private String pdfMobstatFileUrl;
    private String pdfMobstatFileUrlStatus;
    private String printFileUrl;
    private String printFileUrlStatus;
    private String archiveBlobUrl;
    private String archiveStatus;
    private String overallStatus;
}
