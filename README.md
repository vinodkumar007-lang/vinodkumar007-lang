package com.nedbank.kafka.filemanage.util;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.CustomerSummary;
import com.nedbank.kafka.filemanage.model.SummaryPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.*;

public class SummaryJsonWriter {

    private static final Logger logger = LoggerFactory.getLogger(SummaryJsonWriter.class);

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final String azureBlobStorageAccount;

    public SummaryJsonWriter(String azureBlobStorageAccount) {
        this.azureBlobStorageAccount = azureBlobStorageAccount;
    }

    public void writeSummaryJson(SummaryPayload summaryPayload, File outputFile) {
        try {
            Map<String, Object> jsonMap = new LinkedHashMap<>();
            String timestamp = summaryPayload.getHeader().getTimestamp().split(" ")[0].replace(":", "");
            String fileName = summaryPayload.getHeader().getJobName() + "_" + timestamp + ".csv";

            jsonMap.put("batchID", summaryPayload.getHeader().getBatchId());
            jsonMap.put("fileName", fileName);
            jsonMap.put("header", summaryPayload.getHeader());

            List<Map<String, Object>> processedFiles = new ArrayList<>();
            for (CustomerSummary customer : summaryPayload.getMetadata().getCustomerSummaries()) {
                Map<String, Object> custMap = new HashMap<>();
                custMap.put("customerID", customer.getCustomerId());
                custMap.put("accountNumber", customer.getAccountNumber());

                for (CustomerSummary.FileDetail file : customer.getFiles()) {
                    String url = file.getFileUrl();
                    if (url.contains("archive")) {
                        custMap.put("pdfArchiveFileURL", url);
                    } else if (url.contains("email") && url.endsWith(".pdf")) {
                        custMap.put("pdfEmailFileURL", url);
                    } else if (url.contains("html")) {
                        custMap.put("htmlEmailFileURL", url);
                    } else if (url.contains("txt")) {
                        custMap.put("txtEmailFileURL", url);
                    } else if (url.contains("mobstat")) {
                        custMap.put("pdfMobstatFileURL", url);
                    }
                }

                custMap.put("statusCode", "OK");
                custMap.put("statusDescription", "Success");

                processedFiles.add(custMap);
            }

            jsonMap.put("processedFiles", processedFiles);

            List<Map<String, String>> printFiles = new ArrayList<>();
            if (summaryPayload.getPayload().getPrintFiles() != null) {
                for (Object fileObj : summaryPayload.getPayload().getPrintFiles()) {
                    Map<String, String> printMap = new HashMap<>();
                    printMap.put("printFileURL", "https://" + azureBlobStorageAccount + "/pdfs/mobstat/" + fileObj.toString());
                    printFiles.add(printMap);
                }
            }

            jsonMap.put("printFiles", printFiles);

            objectMapper.writerWithDefaultPrettyPrinter().writeValue(outputFile, jsonMap);
            logger.info("Successfully wrote structured summary JSON to: {}", outputFile.getAbsolutePath());

        } catch (IOException e) {
            logger.error("Failed to write summary JSON to file", e);
        }
    }
}
