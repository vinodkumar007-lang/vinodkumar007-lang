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
        if (!headerInitialized) {
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

        for(int i=0; i<payload.getMetaData().getCustomerSummaries().size(); i++) {
            // Add processedFiles entry only if not already added
            String customerKey = payload.getMetaData().getCustomerSummaries().get(i).getCustomerId()
            + "|" + payload.getMetaData().getCustomerSummaries().get(i).getAccountNumber();
            if (!processedCustomerAccounts.contains(customerKey)) {
                ObjectNode processedEntry = mapper.createObjectNode();
                processedEntry.put("customerID", payload.getMetaData().getCustomerSummaries().get(i).getCustomerId());
                processedEntry.put("accountNumber", payload.getMetaData().getCustomerSummaries().get(i).getAccountNumber());
                processedEntry.put("pdfArchiveFileURL", payload.getMetaData().getCustomerSummaries().get(i).getPdfArchiveFileURL());
                processedEntry.put("pdfEmailFileURL", payload.getMetaData().getCustomerSummaries().get(i).getPdfEmailFileURL());
                processedEntry.put("htmlEmailFileURL", payload.getMetaData().getCustomerSummaries().get(i).getHtmlEmailFileURL());
                processedEntry.put("txtEmailFileURL", payload.getMetaData().getCustomerSummaries().get(i).getTxtEmailFileURL());
                processedEntry.put("pdfMobstatFileURL", payload.getMetaData().getCustomerSummaries().get(i).getPdfMobstatFileURL());
                processedEntry.put("statusCode", payload.getStatusCode());
                processedEntry.put("statusDescription", payload.getStatusDescription());

                ((ArrayNode) root.withArray("processedFiles")).add(processedEntry);
                processedCustomerAccounts.add(customerKey);
            }
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
