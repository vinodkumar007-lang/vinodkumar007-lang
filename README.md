public static SummaryPayload buildPayload(
        KafkaMessage kafkaMessage,
        List<SummaryProcessedFile> processedList,
        String summaryBlobUrl,
        String fileName,
        String batchId,
        String timestamp
) {
    SummaryPayload payload = new SummaryPayload();
    payload.setBatchID(batchId);
    payload.setFileName(fileName);
    payload.setTimestamp(timestamp);
    payload.setSummaryFileURL(summaryBlobUrl);

    // HEADER
    Header header = new Header();
    header.setTenantCode(kafkaMessage.getTenantCode());
    header.setChannelID(kafkaMessage.getChannelID());
    header.setAudienceID(kafkaMessage.getAudienceID());
    header.setTimestamp(timestamp);
    header.setSourceSystem(kafkaMessage.getSourceSystem());
    header.setProduct(kafkaMessage.getSourceSystem());
    header.setJobName(kafkaMessage.getSourceSystem());
    payload.setHeader(header);

    // METADATA
    Metadata metadata = new Metadata();
    metadata.setTotalFilesProcessed(processedList.size());
    metadata.setProcessingStatus("Completed");
    metadata.setEventOutcomeCode("0");
    metadata.setEventOutcomeDescription("Success");
    payload.setMetadata(metadata);

    // PAYLOAD BLOCK
    Payload payloadInfo = new Payload();
    payloadInfo.setUniqueConsumerRef(kafkaMessage.getConsumerReference());
    payloadInfo.setUniqueECPBatchRef(null);
    payloadInfo.setRunPriority(null);
    payloadInfo.setEventID(null);
    payloadInfo.setEventType(null);
    payloadInfo.setRestartKey(null);
    payloadInfo.setFileCount(processedList.size());
    payload.setPayload(payloadInfo);

    // PROCESSED FILES
    List<ProcessedFileEntry> processedFileEntries = buildProcessedFileEntries(processedList);
    payload.setProcessedFiles(processedFileEntries);

    // TRIGGER FILE URL IF ANY
    payload.setMobstatTriggerFile(buildMobstatTrigger(processedList));

    return payload;
}

private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> inputList) {
    Map<String, ProcessedFileEntry> customerMap = new LinkedHashMap<>();

    for (SummaryProcessedFile spf : inputList) {
        String key = spf.getCustomerId() + "::" + spf.getAccountNumber();
        ProcessedFileEntry entry = customerMap.computeIfAbsent(key, k -> {
            ProcessedFileEntry pfe = new ProcessedFileEntry();
            pfe.setCustomerId(spf.getCustomerId());
            pfe.setAccountNumber(spf.getAccountNumber());
            return pfe;
        });

        // Map fileType → URL & Status
        String url = spf.getBlobFileURL();
        String status = spf.getStatus() != null ? spf.getStatus() : "UNKNOWN";

        if (url != null && url.contains("/archive/")) {
            entry.setPdfArchiveFileUrl(url);
            entry.setPdfArchiveFileUrlStatus(status);
        } else if (url != null && url.contains("/email/")) {
            entry.setPdfEmailFileUrl(url);
            entry.setPdfEmailFileUrlStatus(status);
        } else if (url != null && url.contains("/html/")) {
            entry.setPrintFileUrl(url);
            entry.setPrintFileUrlStatus(status);
        } else if (url != null && url.contains("/mobstat/")) {
            entry.setPdfMobstatFileUrl(url);
            entry.setPdfMobstatFileUrlStatus(status);
        }

        // Overall statusCode and statusDescription
        entry.setStatusCode("OK");
        entry.setStatusDescription("Success");
    }

    return new ArrayList<>(customerMap.values());
}
private static String buildMobstatTrigger(List<SummaryProcessedFile> list) {
    return list.stream()
        .map(SummaryProcessedFile::getBlobFileURL)
        .filter(url -> url != null && url.contains("/mobstat/DropData.trigger"))
        .findFirst()
        .orElse(null);
}

@Data
public class ProcessedFileEntry {
    private String customerId;
    private String accountNumber;

    private String pdfArchiveFileUrl;
    private String pdfArchiveFileUrlStatus;

    private String pdfEmailFileUrl;
    private String pdfEmailFileUrlStatus;

    private String printFileUrl;
    private String printFileUrlStatus;

    private String pdfMobstatFileUrl;
    private String pdfMobstatFileUrlStatus;

    private String statusCode;
    private String statusDescription;
}
