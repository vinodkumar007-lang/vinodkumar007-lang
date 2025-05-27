private SummaryPayload buildFinalResponse(List<SummaryPayload> payloads) {
    SummaryPayload response = new SummaryPayload();

    List<ProcessedFile> processedFiles = new ArrayList<>();
    List<PrintFile> printFiles = new ArrayList<>();

    for (SummaryPayload payload : payloads) {
        // Deduplicate processedFiles
        if (payload.getProcessedFiles() != null) {
            for (ProcessedFile pf : payload.getProcessedFiles()) {
                if (!processedFiles.contains(pf)) {
                    processedFiles.add(pf);
                }
            }
        }

        // Deduplicate printFiles
        if (payload.getPrintFiles() != null) {
            for (PrintFile pf : payload.getPrintFiles()) {
                if (!printFiles.contains(pf)) {
                    printFiles.add(pf);
                }
            }
        }

        // Set each field only once if not already set
        if (response.getSourceSystem() == null && payload.getSourceSystem() != null)
            response.setSourceSystem(payload.getSourceSystem());

        if (response.getConsumerReference() == null && payload.getConsumerReference() != null)
            response.setConsumerReference(payload.getConsumerReference());

        if (response.getProcessReference() == null && payload.getProcessReference() != null)
            response.setProcessReference(payload.getProcessReference());

        if (response.getTimestamp() == null && payload.getTimestamp() != null)
            response.setTimestamp(payload.getTimestamp());

        if (response.getBlobURL() == null && payload.getBlobURL() != null)
            response.setBlobURL(payload.getBlobURL());

        if (response.getProduct() == null && payload.getProduct() != null)
            response.setProduct(payload.getProduct());

        if (response.getEventOutcomeCode() == null && payload.getEventOutcomeCode() != null)
            response.setEventOutcomeCode(payload.getEventOutcomeCode());

        if (response.getEventOutcomeDescription() == null && payload.getEventOutcomeDescription() != null)
            response.setEventOutcomeDescription(payload.getEventOutcomeDescription());
    }

    response.setProcessedFiles(processedFiles);
    response.setPrintFiles(printFiles);
    return response;
}

public class SummaryJsonWriter {

    private static final ObjectMapper mapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);
    private static final Set<String> processedCustomerAccounts = new HashSet<>();
    private static final Set<String> addedPrintFileUrls = new HashSet<>();
    private static boolean headerInitialized = false;

    public static synchronized void appendToSummaryJson(File summaryFile, SummaryPayload payload) throws IOException {
        ObjectNode root;

        if (summaryFile.exists()) {
            root = (ObjectNode) mapper.readTree(summaryFile);
        } else {
            root = mapper.createObjectNode();
            root.putArray("processedFiles");
            root.putArray("printFiles");
        }

        // Set batchID and fileName only once
        if (!root.has("batchID")) {
            root.put("batchID", payload.getBatchID());
        }
        if (!root.has("fileName")) {
            root.put("fileName", payload.getFileName());
        }

        // Header - set once only
        if (!headerInitialized) {
            ObjectNode header = mapper.createObjectNode();
            header.put("tenantCode", payload.getTenantCode());
            header.put("channelID", payload.getChannelID());
            header.put("audienceID", payload.getAudienceID());
            header.put("timestamp", payload.getTimestamp());
            header.put("sourceSystem", payload.getSourceSystem());
            header.put("product", payload.getProduct());
            header.put("jobName", payload.getJobName());
            root.set("header", header);
            headerInitialized = true;
        }

        // Add processedFiles entry only if not already added
        String customerKey = payload.getCustomerID() + "|" + payload.getAccountNumber();
        if (!processedCustomerAccounts.contains(customerKey)) {
            ObjectNode processedEntry = mapper.createObjectNode();
            processedEntry.put("customerID", payload.getCustomerID());
            processedEntry.put("accountNumber", payload.getAccountNumber());
            processedEntry.put("pdfArchiveFileURL", payload.getPdfArchiveFileURL());
            processedEntry.put("pdfEmailFileURL", payload.getPdfEmailFileURL());
            processedEntry.put("htmlEmailFileURL", payload.getHtmlEmailFileURL());
            processedEntry.put("txtEmailFileURL", payload.getTxtEmailFileURL());
            processedEntry.put("pdfMobstatFileURL", payload.getPdfMobstatFileURL());
            processedEntry.put("statusCode", payload.getStatusCode());
            processedEntry.put("statusDescription", payload.getStatusDescription());

            ((ArrayNode) root.withArray("processedFiles")).add(processedEntry);
            processedCustomerAccounts.add(customerKey);
        }

        // Add print file if not already added
        if (payload.getPrintFileURL() != null && !addedPrintFileUrls.contains(payload.getPrintFileURL())) {
            ObjectNode printFileEntry = mapper.createObjectNode();
            printFileEntry.put("printFileURL", payload.getPrintFileURL());

            ((ArrayNode) root.withArray("printFiles")).add(printFileEntry);
            addedPrintFileUrls.add(payload.getPrintFileURL());
        }

        // Write back to file
        mapper.writeValue(summaryFile, root);
    }
}
