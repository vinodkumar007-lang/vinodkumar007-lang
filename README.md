package com.nedbank.kafka.filemanage.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nedbank.kafka.filemanage.model.CustomerSummary;
import com.nedbank.kafka.filemanage.model.HeaderInfo;
import com.nedbank.kafka.filemanage.model.SummaryPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class SummaryJsonWriter {

    private static final Logger logger = LoggerFactory.getLogger(SummaryJsonWriter.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void writeUpdatedSummaryJson(SummaryPayload payload, File summaryFile, String azureBlobStorageAccount) {
        try {
            ObjectNode rootNode = mapper.createObjectNode();

            HeaderInfo header = payload.getHeader();
            rootNode.put("batchID", header.getBatchId());
            rootNode.put("fileName", header.getJobName() + "_" + header.getBatchId() + ".csv");

            // Header info
            ObjectNode headerNode = mapper.createObjectNode();
            headerNode.put("tenantCode", header.getTenantCode());
            headerNode.put("channelID", header.getChannelID());
            headerNode.put("audienceID", header.getAudienceID());
            headerNode.put("timestamp", header.getTimestamp());
            headerNode.put("sourceSystem", header.getSourceSystem());
            headerNode.put("product", header.getProduct());
            headerNode.put("jobName", header.getJobName());
            rootNode.set("header", headerNode);

            // Processed Files
            List<ObjectNode> processedList = new ArrayList<>();
            for (CustomerSummary customer : payload.getMetadata().getCustomerSummaries()) {
                ObjectNode custNode = mapper.createObjectNode();
                custNode.put("customerID", customer.getCustomerId());
                custNode.put("accountNumber", customer.getAccountNumber() != null ? customer.getAccountNumber() : "");

                customer.getFiles().forEach(file -> {
                    String path = file.getFileLocation().toLowerCase();
                    if (path.contains("archive")) {
                        custNode.put("pdfArchiveFileURL", "https://" + azureBlobStorageAccount + "/" + file.getFileLocation());
                    } else if (path.contains("email") && path.endsWith(".pdf")) {
                        custNode.put("pdfEmailFileURL", "https://" + azureBlobStorageAccount + "/" + file.getFileLocation());
                    } else if (path.contains("email") && path.endsWith(".html")) {
                        custNode.put("htmlEmailFileURL", "https://" + azureBlobStorageAccount + "/" + file.getFileLocation());
                    } else if (path.contains("email") && path.endsWith(".txt")) {
                        custNode.put("txtEmailFileURL", "https://" + azureBlobStorageAccount + "/" + file.getFileLocation());
                    } else if (path.contains("mobstat") && path.endsWith(".pdf")) {
                        custNode.put("pdfMobstatFileURL", "https://" + azureBlobStorageAccount + "/" + file.getFileLocation());
                    }
                });

                custNode.put("statusCode", "OK");
                custNode.put("statusDescription", "Success");
                processedList.add(custNode);
            }
            rootNode.set("processedFiles", mapper.valueToTree(processedList));

            // Print Files
            List<Map<String, String>> printFilesList = new ArrayList<>();
            if (payload.getPayload().getPrintFiles() != null) {
                for (Object printFileObj : payload.getPayload().getPrintFiles()) {
                    if (printFileObj instanceof String printFilePath) {
                        Map<String, String> map = new HashMap<>();
                        map.put("printFileURL", "https://" + azureBlobStorageAccount + "/" + printFilePath);
                        printFilesList.add(map);
                    }
                }
            }
            rootNode.set("printFiles", mapper.valueToTree(printFilesList));

            // Write to file
            mapper.writerWithDefaultPrettyPrinter().writeValue(summaryFile, rootNode);
            logger.info("Successfully wrote updated summary to {}", summaryFile.getAbsolutePath());

        } catch (IOException e) {
            logger.error("Failed to write summary.json", e);
        }
    }
}
