package com.nedbank.kafka.filemanage.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nedbank.kafka.filemanage.model.CustomerSummary;
import com.nedbank.kafka.filemanage.model.SummaryPayload;
import com.nedbank.kafka.filemanage.model.PayloadInfo;
import com.nedbank.kafka.filemanage.model.HeaderInfo;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class SummaryJsonWriter {
    private static final Logger logger = LoggerFactory.getLogger(SummaryJsonWriter.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void writeUpdatedSummaryJson(File summaryFile, SummaryPayload payload, String azureBlobStorageAccount) {
        try {
            ObjectNode rootNode = mapper.createObjectNode();

            // Batch ID
            rootNode.put("batchID", payload.getHeader().getBatchId());

            // File name (assume naming convention)
            String fileName = "DEBTMAN_" + new SimpleDateFormat("yyyyMMdd").format(new Date()) + ".csv";
            rootNode.put("fileName", fileName);

            // Header block
            ObjectNode headerNode = mapper.createObjectNode();
            headerNode.put("tenantCode", payload.getHeader().getTenantCode());
            headerNode.put("channelID", payload.getHeader().getChannelID());
            headerNode.put("audienceID", payload.getHeader().getAudienceID());
            headerNode.put("timestamp", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(new Date()));
            headerNode.put("sourceSystem", payload.getHeader().getSourceSystem());
            headerNode.put("product", "DEBTMANAGER");
            headerNode.put("jobName", payload.getHeader().getJobName());
            rootNode.set("header", headerNode);

            // Processed files
            ArrayNode processedFiles = mapper.createArrayNode();
            for (CustomerSummary customer : payload.getMetaData().getCustomerSummaries()) {
                ObjectNode custNode = mapper.createObjectNode();
                custNode.put("customerID", customer.getCustomerId());
                custNode.put("accountNumber", customer.getAccountNumber());

                String accountId = customer.getAccountNumber();
                String batchId = payload.getHeader().getBatchId();

                // Add document URLs
                custNode.put("pdfArchiveFileURL", buildBlobUrl(azureBlobStorageAccount, "pdfs/archive", accountId, batchId, "pdf"));
                custNode.put("pdfEmailFileURL", buildBlobUrl(azureBlobStorageAccount, "pdfs/email", accountId, batchId, "pdf"));
                custNode.put("htmlEmailFileURL", buildBlobUrl(azureBlobStorageAccount, "pdfs/html", accountId, batchId, "html"));
                custNode.put("txtEmailFileURL", buildBlobUrl(azureBlobStorageAccount, "pdfs/txt", accountId, batchId, "txt"));
                custNode.put("pdfMobstatFileURL", buildBlobUrl(azureBlobStorageAccount, "pdfs/mobstat", accountId, batchId, "pdf"));

                custNode.put("statusCode", "OK");
                custNode.put("statusDescription", "Success");

                processedFiles.add(custNode);
            }
            rootNode.set("processedFiles", processedFiles);

            // Print files
            ArrayNode printFilesNode = mapper.createArrayNode();
            List<String> printFiles = payload.getPayload().getPrintFiles();
            if (printFiles != null) {
                for (String printFileName : printFiles) {
                    ObjectNode printNode = mapper.createObjectNode();
                    printNode.put("printFileURL", buildBlobUrl(azureBlobStorageAccount, "pdfs/mobstat", printFileName, payload.getHeader().getBatchId(), "ps"));
                    printFilesNode.add(printNode);
                }
            }
            rootNode.set("printFiles", printFilesNode);

            // Write to file
            mapper.writerWithDefaultPrettyPrinter().writeValue(summaryFile, rootNode);
            logger.info("Generated structured summary.json at {}", summaryFile.getAbsolutePath());

        } catch (IOException e) {
            logger.error("Error writing structured summary.json", e);
        }
    }

    private static String buildBlobUrl(String account, String path, String id, String batchId, String ext) {
        return String.format("https://%s/%s/%s_%s.%s", account, path, id, batchId, ext);
    }
}
