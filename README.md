public String uploadSummaryJson(     String sourceSystem,
    String batchId,
    String timestamp,
    String summaryJsonContent )

    public String copyFileFromUrlToBlob(     String sourceUrl,
    String sourceSystem,
    String batchId,
    String consumerReference,
    String processReference,
    String timestamp,
    String fileName )

    File summaryFile = SummaryJsonWriter.appendToSummaryJson(batchId, header, processedFiles, printFiles);
package com.nedbank.kafka.filemanage.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nedbank.kafka.filemanage.model.SummaryPayload;

import java.io.File;
import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

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
            root.put("batchID", payload.getBatchId());
        }
        if (!root.has("fileName")) {
            root.put("fileName", payload.getFileName());
        }

        // Header - set once only
        if (!headerInitialized && payload.getHeader() != null) {
            ObjectNode header = mapper.createObjectNode();
            header.put("tenantCode", payload.getHeader().getTenantCode());
            header.put("channelID", payload.getHeader().getChannelID());
            header.put("audienceID", payload.getHeader().getAudienceID());
            header.put("timestamp", payload.getHeader().getTimestamp());
            header.put("sourceSystem", payload.getHeader().getSourceSystem());
            header.put("product", payload.getHeader().getProduct());
            header.put("jobName", payload.getJobName());
            root.set("header", header);
            headerInitialized = true;
        }

        // Process customer summaries
        if (payload.getMetaData() != null && payload.getMetaData().getCustomerSummaries() != null) {
            for (var summary : payload.getMetaData().getCustomerSummaries()) {
                String customerKey = summary.getCustomerId() + "|" + summary.getAccountNumber();

                // Add processedFiles entry only if not already added
                if (!processedCustomerAccounts.contains(customerKey)) {
                    ObjectNode processedEntry = mapper.createObjectNode();
                    processedEntry.put("customerID", summary.getCustomerId());
                    processedEntry.put("accountNumber", summary.getAccountNumber());
                    processedEntry.put("pdfArchiveFileURL", summary.getPdfArchiveFileURL());
                    processedEntry.put("pdfEmailFileURL", summary.getPdfEmailFileURL());
                    processedEntry.put("htmlEmailFileURL", summary.getHtmlEmailFileURL());
                    processedEntry.put("txtEmailFileURL", summary.getTxtEmailFileURL());
                    processedEntry.put("pdfMobstatFileURL", summary.getPdfMobstatFileURL());
                    processedEntry.put("statusCode", payload.getStatusCode());
                    processedEntry.put("statusDescription", payload.getStatusDescription());

                    ((ArrayNode) root.withArray("processedFiles")).add(processedEntry);
                    processedCustomerAccounts.add(customerKey);
                }

                // Add print file if not already added
                if (summary.getPrintFileURL() != null && !addedPrintFileUrls.contains(summary.getPrintFileURL())) {
                    ObjectNode printFileEntry = mapper.createObjectNode();
                    printFileEntry.put("printFileURL", summary.getPrintFileURL());

                    ((ArrayNode) root.withArray("printFiles")).add(printFileEntry);
                    addedPrintFileUrls.add(summary.getPrintFileURL());
                }
            }
        }

        // Write back to file
        mapper.writeValue(summaryFile, root);
    }
}
