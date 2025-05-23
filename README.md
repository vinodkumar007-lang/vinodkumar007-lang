java.io.IOException: No batch files found
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.processMessages(KafkaListenerService.java:126) ~[classes/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.processAllMessages(KafkaListenerService.java:84) ~[classes/:na]
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
	at org.springframework.web.filter.FormContentFilter.doFilterInternal(FormContentFilter.java:93) ~[spring-web-6.0.2.jar:6.0.2]
	at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:116) ~[spring-web-6.0.2.jar:6.0.2]
	at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:185) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:158) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.springframework.web.filter.CharacterEncodingFilter.doFilterInternal(CharacterEncodingFilter.java:201) ~[spring-web-6.0.2.jar:6.0.2]
	at org.springframework.web.filter.OncePerRequestFilter.doFilter(OncePerRequestFilter.java:116) ~[spring-web-6.0.2.jar:6.0.2]
	at org.apache.catalina.core.ApplicationFilterChain.internalDoFilter(ApplicationFilterChain.java:185) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.catalina.core.ApplicationFilterChain.doFilter(ApplicationFilterChain.java:158) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.catalina.core.StandardWrapperValve.invoke(StandardWrapperValve.java:197) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.catalina.core.StandardContextValve.invoke(StandardContextValve.java:97) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.catalina.authenticator.AuthenticatorBase.invoke(AuthenticatorBase.java:542) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.catalina.core.StandardHostValve.invoke(StandardHostValve.java:119) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.catalina.valves.ErrorReportValve.invoke(ErrorReportValve.java:92) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.catalina.core.StandardEngineValve.invoke(StandardEngineValve.java:78) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.catalina.connector.CoyoteAdapter.service(CoyoteAdapter.java:357) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.coyote.http11.Http11Processor.service(Http11Processor.java:400) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.coyote.AbstractProcessorLight.process(AbstractProcessorLight.java:65) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.coyote.AbstractProtocol$ConnectionHandler.process(AbstractProtocol.java:861) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.tomcat.util.net.NioEndpoint$SocketProcessor.doRun(NioEndpoint.java:1739) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.tomcat.util.net.SocketProcessorBase.run(SocketProcessorBase.java:52) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.tomcat.util.threads.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1191) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.tomcat.util.threads.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:659) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at org.apache.tomcat.util.threads.TaskThread$WrappingRunnable.run(TaskThread.java:61) ~[tomcat-embed-core-10.1.1.jar:10.1.1]
	at java.base/java.lang.Thread.run(Thread.java:842) ~[na:na]

package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.CustomerSummary;
import com.nedbank.kafka.filemanage.model.HeaderInfo;
import com.nedbank.kafka.filemanage.model.MetaDataInfo;
import com.nedbank.kafka.filemanage.model.PayloadInfo;
import com.nedbank.kafka.filemanage.model.SummaryPayload;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.util.*;

@Service
public class KafkaListenerService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final BlobStorageService blobStorageService;
    private final ConsumerFactory<String, String> consumerFactory;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${kafka.topic.input}")
    private String inputTopic;

    @Value("${kafka.topic.output}")
    private String outputTopic;

    @Value("${azure.blob.storage.account}")
    private String azureBlobStorageAccount;

    public KafkaListenerService(KafkaTemplate<String, String> kafkaTemplate,
                                BlobStorageService blobStorageService,
                                ConsumerFactory<String, String> consumerFactory) {
        this.kafkaTemplate = kafkaTemplate;
        this.blobStorageService = blobStorageService;
        this.consumerFactory = consumerFactory;
    }

    public Map<String, Object> processAllMessages() {
        Consumer<String, String> consumer = consumerFactory.createConsumer();
        try {
            List<TopicPartition> partitions = consumer.partitionsFor(inputTopic).stream()
                    .map(info -> new TopicPartition(info.topic(), info.partition()))
                    .toList();

            consumer.assign(partitions);
            consumer.poll(Duration.ofMillis(100));
            consumer.seekToBeginning(partitions);

            List<String> allMessages = new ArrayList<>();
            int emptyPollCount = 0;

            while (emptyPollCount < 3) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(1));
                if (records.isEmpty()) {
                    emptyPollCount++;
                } else {
                    emptyPollCount = 0;
                    for (ConsumerRecord<String, String> record : records) {
                        allMessages.add(record.value());
                    }
                }
            }

            if (allMessages.isEmpty()) {
                return generateErrorResponse("204", "No content processed from Kafka");
            }

            // Process all messages, build summary payload
            SummaryPayload summaryPayload = processMessages(allMessages);

            // Write summary to JSON file
            File jsonFile = writeSummaryToFile(summaryPayload);

            // Send final response to Kafka
            sendFinalResponseToKafka(summaryPayload, jsonFile);

            // Return final enriched response
            Map<String, Object> response = new HashMap<>();
            response.put("message", "Batch processed successfully");
            response.put("status", "success");
            response.put("summaryPayload", summaryPayload);

            return response;

        } catch (Exception e) {
            logger.error("Error during Kafka message processing", e);
            return generateErrorResponse("500", "Internal Server Error while processing messages");
        } finally {
            consumer.close();
        }
    }

    // --- NEW METHOD: processes all messages and aggregates into one SummaryPayload ---
    private SummaryPayload processMessages(List<String> allMessages) throws IOException {
        List<CustomerSummary> customerSummaries = new ArrayList<>();
        String fileName = "";
        String jobName = "";
        String batchId = null;

        Set<String> archived = new HashSet<>();
        Set<String> emailed = new HashSet<>();
        Set<String> mobstat = new HashSet<>();
        Set<String> printed = new HashSet<>();

        for (String message : allMessages) {
            JsonNode root = objectMapper.readTree(message);
            if (batchId == null) batchId = extractField(root, "consumerReference");

            JsonNode batchFilesNode = root.get("BatchFiles");
            if (batchFilesNode == null || !batchFilesNode.isArray() || batchFilesNode.isEmpty()) {
                throw new IOException("No batch files found");
            }

            JsonNode firstFile = batchFilesNode.get(0);
            String filePath = firstFile.get("fileLocation").asText();
            String objectId = firstFile.get("ObjectId").asText();

            // Upload & get SAS URL per file (optional usage or just for first?)
            try {
                blobStorageService.uploadFileAndGenerateSasUrl(filePath, batchId, objectId);
            } catch (Exception e) {
                logger.warn("Failed to generate SAS URL for file " + filePath, e);
            }

            for (JsonNode fileNode : batchFilesNode) {
                String objId = fileNode.get("ObjectId").asText();
                String location = fileNode.get("fileLocation").asText();
                String extension = getFileExtension(location).toLowerCase();
                String customerId = objId.split("_")[0];

                if (fileNode.has("fileName")) fileName = fileNode.get("fileName").asText();
                if (fileNode.has("jobName")) jobName = fileNode.get("jobName").asText();

                CustomerSummary.FileDetail detail = new CustomerSummary.FileDetail();
                detail.setObjectId(objId);
                detail.setFileLocation(location);
                detail.setFileUrl("https://" + azureBlobStorageAccount + "/" + location);
                detail.setStatus(extension.equals(".ps") ? "failed" : "OK");
                detail.setEncrypted(isEncrypted(location, extension));
                detail.setType(determineType(location, extension));

                if (location.contains("mobstat")) mobstat.add(customerId);
                if (location.contains("archive")) archived.add(customerId);
                if (location.contains("email")) emailed.add(customerId);
                if (extension.equals(".ps")) printed.add(customerId);

                CustomerSummary customer = customerSummaries.stream()
                        .filter(c -> c.getCustomerId().equals(customerId))
                        .findFirst()
                        .orElseGet(() -> {
                            CustomerSummary c = new CustomerSummary();
                            c.setCustomerId(customerId);
                            c.setAccountNumber("");
                            c.setFiles(new ArrayList<>());
                            customerSummaries.add(c);
                            return c;
                        });

                customer.getFiles().add(detail);
            }
        }

        List<Map<String, Object>> processedFiles = new ArrayList<>();
        for (CustomerSummary customer : customerSummaries) {
            Map<String, Object> pf = new HashMap<>();
            pf.put("customerID", customer.getCustomerId());
            pf.put("accountNumber", customer.getAccountNumber());

            for (CustomerSummary.FileDetail detail : customer.getFiles()) {
                String key = switch (detail.getType()) {
                    case "pdf_archive" -> "pdfArchiveFileURL";
                    case "pdf_email" -> "pdfEmailFileURL";
                    case "html_email" -> "htmlEmailFileURL";
                    case "txt_email" -> "txtEmailFileURL";
                    case "pdf_mobstat" -> "pdfMobstatFileURL";
                    default -> null;
                };
                if (key != null) {
                    pf.put(key, detail.getFileUrl());
                }
            }

            pf.put("statusCode", "OK");
            pf.put("statusDescription", "Success");
            processedFiles.add(pf);
        }

        List<Map<String, Object>> printFiles = new ArrayList<>();
        for (CustomerSummary customer : customerSummaries) {
            for (CustomerSummary.FileDetail detail : customer.getFiles()) {
                if ("ps_print".equals(detail.getType())) {
                    Map<String, Object> pf = new HashMap<>();
                    pf.put("printFileURL", "https://" + azureBlobStorageAccount + "/pdfs/mobstat/" + detail.getObjectId());
                    printFiles.add(pf);
                }
            }
        }

        // Build the header using the first message JSON root for consistency
        HeaderInfo headerInfo = null;
        if (!allMessages.isEmpty()) {
            JsonNode firstRoot = objectMapper.readTree(allMessages.get(0));
            headerInfo = buildHeader(firstRoot, jobName);
        } else {
            headerInfo = new HeaderInfo();
        }

        PayloadInfo payloadInfo = new PayloadInfo();
        payloadInfo.setProcessedFiles(processedFiles);
        payloadInfo.setPrintFiles(printFiles);

        MetaDataInfo metaDataInfo = new MetaDataInfo();
        metaDataInfo.setCustomerSummaries(customerSummaries);

        SummaryPayload summaryPayload = new SummaryPayload();
        summaryPayload.setHeader(headerInfo);
        summaryPayload.setPayload(payloadInfo);
        summaryPayload.setMetadata(metaDataInfo);

        return summaryPayload;
    }

    // --- NEW METHOD: write summary JSON file ---
    private File writeSummaryToFile(SummaryPayload summaryPayload) throws IOException {
        String userHome = System.getProperty("user.home");
        File jsonFile = new File(userHome, "summary.json");

        Map<String, Object> summaryData = new HashMap<>();
        summaryData.put("batchID", summaryPayload.getHeader().getBatchId());
        summaryData.put("fileName", summaryPayload.getHeader().getJobName());
        summaryData.put("header", summaryPayload.getHeader());
        summaryData.put("processedFiles", summaryPayload.getPayload().getProcessedFiles());
        summaryData.put("printFiles", summaryPayload.getPayload().getPrintFiles());

        objectMapper.writerWithDefaultPrettyPrinter().writeValue(jsonFile, summaryData);

        // Add summary file path to meta data
        summaryPayload.setSummaryFileURL(jsonFile.getAbsolutePath());
        summaryPayload.getMetadata().setSummaryFileURL(jsonFile.getAbsolutePath());

        return jsonFile;
    }

    // --- NEW METHOD: send final enriched response to Kafka ---
    private void sendFinalResponseToKafka(SummaryPayload summaryPayload, File jsonFile) throws JsonProcessingException {
        Map<String, Object> kafkaMsg = new HashMap<>();
        kafkaMsg.put("fileName", summaryPayload.getHeader().getJobName());
        kafkaMsg.put("jobName", summaryPayload.getHeader().getJobName());
        kafkaMsg.put("batchId", summaryPayload.getHeader().getBatchId());
        kafkaMsg.put("timestamp", new Date().toString());
        kafkaMsg.put("summaryFileURL", jsonFile.getAbsolutePath());

        // Send a simple message with summary file info
        kafkaTemplate.send(outputTopic, summaryPayload.getHeader().getBatchId(), objectMapper.writeValueAsString(kafkaMsg));

        // Send the detailed enriched summaryPayload as well
        Map<String, Object> response = new HashMap<>();
        response.put("message", "Batch processed successfully");
        response.put("status", "success");
        response.put("summaryPayload", summaryPayload);

        kafkaTemplate.send(outputTopic, summaryPayload.getHeader().getBatchId(), objectMapper.writeValueAsString(response));
    }

    // === EXISTING METHODS (unchanged) ===

    private HeaderInfo buildHeader(JsonNode root, String jobName) {
        HeaderInfo headerInfo = new HeaderInfo();
        headerInfo.setBatchId(extractField(root, "consumerReference"));
        headerInfo.setTenantCode(extractField(root, "tenantCode"));
        headerInfo.setChannelID(extractField(root, "channelId"));
        headerInfo.setAudienceID(extractField(root, "audienceId"));
        headerInfo.setTimestamp(new Date().toString());
        headerInfo.setSourceSystem(extractField(root, "sourceSystem"));
        headerInfo.setProduct(extractField(root, "product"));
        headerInfo.setJobName(jobName);
        return headerInfo;
    }

    private String extractField(JsonNode root, String fieldName) {
        JsonNode fieldNode = root.get(fieldName);
        return fieldNode != null ? fieldNode.asText() : null;
    }

    private String convertPojoToJson(String pojo) {
        return pojo;
    }

    private Map<String, Object> generateErrorResponse(String code, String message) {
        Map<String, Object> error = new HashMap<>();
        error.put("statusCode", code);
        error.put("statusMessage", message);
        return Collections.singletonMap("error", error);
    }

    private boolean isEncrypted(String location, String extension) {
        return extension.equals(".pdf") && location.contains("encrypted");
    }

    private String determineType(String location, String extension) {
        if (extension.equals(".pdf") && location.contains("archive")) return "pdf_archive";
        if (extension.equals(".pdf") && location.contains("email")) return "pdf_email";
        if (extension.equals(".html") && location.contains("email")) return "html_email";
        if (extension.equals(".txt") && location.contains("email")) return "txt_email";
        if (extension.equals(".pdf") && location.contains("mobstat")) return "pdf_mobstat";
        if (extension.equals(".ps")) return "ps_print";
        return "unknown";
    }

    private String getFileExtension(String path) {
        int index = path.lastIndexOf('.');
        return (index >= 0) ? path.substring(index).toLowerCase() : "";
    }
}
