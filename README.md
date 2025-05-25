java.lang.NullPointerException: Cannot invoke "java.util.List.iterator()" because the return value of "com.nedbank.kafka.filemanage.model.MetaDataInfo.getCustomerSummaries()" is null
	at com.nedbank.kafka.filemanage.utils.SummaryJsonWriter.appendToSummaryJson(SummaryJsonWriter.java:51) ~[classes/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.listen(KafkaListenerService.java:97) ~[classes/:na]
	at com.nedbank.kafka.filemanage.controller.FileProcessingController.triggerFileProcessing(FileProcessingController.java:31) ~[classes/:na]
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[na:na]
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77) ~[na:na]
	at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[na:na]
	at java.base/java.lang.reflect.Method.invoke(Method.java:568) ~[na:na]
	at org.springframework.web.method.support.InvocableHandlerMethod.doInvoke(InvocableHandlerMethod.java:207) ~[spring-web-6.0.2.jar:6.0.2]
	at org.springframework.web.method.support.InvocableHandlerMethod.invokeForRequest(InvocableHandlerMethod.java:152) ~[spring-web-6.0.2.jar:6.0.2]
	at org.springframework.web.servlet.mvc.method.annotation.ServletInvocableHandlerMethod.invokeAndHandle(ServletInvocableHandlerMethod.java:117) ~[spring-webmvc-6.0.2.jar:6.0.2]
	at org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter.invokeHandlerMethod(RequestMappingHandlerAdapter.java:884) ~[spring-webmvc-6.0.2.jar:6.0.2]
	at org.springframework.web.servlet.mvc.method.annotation.RequestMappingHandlerAdapter.handleInternal(RequestMappingHandlerAdapter.java:797) ~[spring-webmvc-6.0.2.jar:6.0.2]
	at org.springframework.web.servlet.mvc.method.AbstractHandlerMethodAdapter.handle(AbstractHandlerMethodAdapter.java:87) ~[spring-webmvc-6.0.2.jar:6.0.2]
	at org.springframework.web.servlet.DispatcherServlet.doDispatch(DispatcherServlet.java:1080) ~[spring-webmvc-6.0.2.jar:6.0.2]
	at org.springframework.web.servlet.DispatcherServlet.doService(DispatcherServlet.java:973) ~[spring-webmvc-6.0.2.jar:6.0.2]
	at org.springframework.web.servlet.FrameworkServlet.processRequest(FrameworkServlet.java:1003) ~[spring-webmvc-6.0.2.jar:6.0.2]
	at org.springframework.web.servlet.FrameworkServlet.doPost(FrameworkServlet.java:906) ~[spring-webmvc-6.0.2.jar:6.0.2]
	at jakarta.servlet.http.HttpServlet.service(HttpServlet.java:731) ~[tomcat-embed-core-10.1.1.jar:6.0]
	at org.springframework.web.servlet.FrameworkServlet.service(FrameworkServlet.java:880) ~[spring-webmvc-6.0.2.jar:6.0.2]
	at jakarta.servlet.http.HttpServlet.service(HttpServlet.java:814) ~[tomcat-embed-core-10.1.1.jar:6.0]
	at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:223) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:158) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.tomcat.websocket.server.WsFilter.doFilter(WsFilter.java:53) ~[tomcat-embed-websocket-10.1.1.jar:10.1.1]
	at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:185) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:158) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.springframework.web.filter.RequestContextFilter.doFilterInternal(RequestContextFilter.java:100) ~[spring-web-6.0.2.jar:6.0.2]
	at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:116) ~[spring-web-6.0.2.jar:6.0.2]
	at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:185) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:158) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.springframework.w

 package com.nedbank.kafka.filemanage.utils;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.nedbank.kafka.filemanage.model.CustomerSummary;
import com.nedbank.kafka.filemanage.model.SummaryPayload;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class SummaryJsonWriter {
    private static final Logger logger = LoggerFactory.getLogger(SummaryJsonWriter.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public static void appendToSummaryJson(File summaryFile, SummaryPayload newPayload, String azureBlobStorageAccount) {
        try {
            ObjectNode root;
            if (summaryFile.exists()) {
                root = (ObjectNode) mapper.readTree(summaryFile);
            } else {
                root = mapper.createObjectNode();
                root.put("batchID", newPayload.getHeader().getBatchId());
                root.put("fileName", "DEBTMAN_" + new SimpleDateFormat("yyyyMMdd").format(new Date()) + ".csv");

                // Properly populate header block
                ObjectNode headerNode = mapper.createObjectNode();
                headerNode.put("tenantCode", newPayload.getHeader().getTenantCode());
                headerNode.put("channelID", newPayload.getHeader().getChannelID());
                headerNode.put("audienceID", newPayload.getHeader().getAudienceID());
                headerNode.put("timestamp", new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss'Z'").format(new Date()));
                headerNode.put("sourceSystem", newPayload.getHeader().getSourceSystem());
                headerNode.put("product", "DEBTMANAGER");
                headerNode.put("jobName", newPayload.getHeader().getJobName());
                root.set("header", headerNode);

                root.set("processedFiles", mapper.createArrayNode());
                root.set("printFiles", mapper.createArrayNode());
            }

            // Append processedFiles
            ArrayNode processedFiles = (ArrayNode) root.withArray("processedFiles");
            for (CustomerSummary customer : newPayload.getMetaData().getCustomerSummaries()) {
                ObjectNode custNode = mapper.createObjectNode();
                custNode.put("customerID", customer.getCustomerId());
                custNode.put("accountNumber", customer.getAccountNumber());
                String acc = customer.getAccountNumber();
                String batchId = newPayload.getHeader().getBatchId();
                custNode.put("pdfArchiveFileURL", buildBlobUrl(azureBlobStorageAccount, "pdfs/archive", acc, batchId, "pdf"));
                custNode.put("pdfEmailFileURL", buildBlobUrl(azureBlobStorageAccount, "pdfs/email", acc, batchId, "pdf"));
                custNode.put("htmlEmailFileURL", buildBlobUrl(azureBlobStorageAccount, "pdfs/html", acc, batchId, "html"));
                custNode.put("txtEmailFileURL", buildBlobUrl(azureBlobStorageAccount, "pdfs/txt", acc, batchId, "txt"));
                custNode.put("pdfMobstatFileURL", buildBlobUrl(azureBlobStorageAccount, "pdfs/mobstat", acc, batchId, "pdf"));
                custNode.put("statusCode", "OK");
                custNode.put("statusDescription", "Success");
                processedFiles.add(custNode);
            }

            // Append print files
            ArrayNode printFiles = (ArrayNode) root.withArray("printFiles");
            for (String pf : newPayload.getPayload().getPrintFiles()) {
                ObjectNode pfNode = mapper.createObjectNode();
                pfNode.put("printFileURL", buildBlobUrl(azureBlobStorageAccount, "pdfs/mobstat", pf, newPayload.getHeader().getBatchId(), "ps"));
                printFiles.add(pfNode);
            }

            mapper.writerWithDefaultPrettyPrinter().writeValue(summaryFile, root);
            logger.info("Appended to summary.json: {}", summaryFile.getAbsolutePath());

        } catch (IOException e) {
            logger.error("Error appending to summary.json", e);
        }
    }

    // --- Existing private method unchanged ---
    private static ObjectNode buildPayloadNode(SummaryPayload payload, String azureBlobStorageAccount) {
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

        return rootNode;
    }

    // --- New method added to merge existing JSON with incoming ---
    private static ObjectNode mergeSummaryJson(ObjectNode existing, ObjectNode incoming) {
        // Merge processedFiles arrays without duplicates by customerID
        ArrayNode existingFiles = (ArrayNode) existing.get("processedFiles");
        if (existingFiles == null) {
            existingFiles = mapper.createArrayNode();
            existing.set("processedFiles", existingFiles);
        }

        ArrayNode incomingFiles = (ArrayNode) incoming.get("processedFiles");
        Set<String> existingCustomerIds = new HashSet<>();
        for (JsonNode node : existingFiles) {
            existingCustomerIds.add(node.get("customerID").asText());
        }

        if (incomingFiles != null) {
            for (JsonNode node : incomingFiles) {
                String custId = node.get("customerID").asText();
                if (!existingCustomerIds.contains(custId)) {
                    existingFiles.add(node);
                    existingCustomerIds.add(custId);
                }
            }
        }

        // Merge printFiles arrays without duplicates by URL
        ArrayNode existingPrintFiles = (ArrayNode) existing.get("printFiles");
        if (existingPrintFiles == null) {
            existingPrintFiles = mapper.createArrayNode();
            existing.set("printFiles", existingPrintFiles);
        }

        ArrayNode incomingPrintFiles = (ArrayNode) incoming.get("printFiles");
        Set<String> existingPrintFileUrls = new HashSet<>();
        for (JsonNode node : existingPrintFiles) {
            existingPrintFileUrls.add(node.get("printFileURL").asText());
        }

        if (incomingPrintFiles != null) {
            for (JsonNode node : incomingPrintFiles) {
                String url = node.get("printFileURL").asText();
                if (!existingPrintFileUrls.contains(url)) {
                    existingPrintFiles.add(node);
                    existingPrintFileUrls.add(url);
                }
            }
        }

        // Update header timestamp with latest (incoming)
        ObjectNode existingHeader = (ObjectNode) existing.get("header");
        ObjectNode incomingHeader = (ObjectNode) incoming.get("header");
        if (existingHeader != null && incomingHeader != null) {
            existingHeader.put("timestamp", incomingHeader.get("timestamp").asText());
        }

        // Optionally you can merge other header fields here

        return existing;
    }

    // --- Existing helper method unchanged ---
    private static String buildBlobUrl(String account, String path, String id, String batchId, String ext) {
        return String.format("https://%s/%s/%s_%s.%s", account, path, id, batchId, ext);
    }
}
