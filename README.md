package com.nedbank.kafka.filemanage.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.nedbank.kafka.filemanage.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;

@Component
public class SummaryJsonWriter {

    private static final Logger logger = LoggerFactory.getLogger(SummaryJsonWriter.class);
    private static final ObjectMapper objectMapper = new ObjectMapper().enable(SerializationFeature.INDENT_OUTPUT);

    public static String writeSummaryJsonToFile(SummaryPayload payload) {
        if (payload == null) {
            logger.error("SummaryPayload is null. Cannot write summary.json.");
            throw new IllegalArgumentException("SummaryPayload cannot be null");
        }

        try {
            String batchId = Optional.ofNullable(payload.getBatchID()).orElse("unknown");
            String fileName = "summary_" + batchId + ".json";

            Path tempDir = Files.createTempDirectory("summaryFiles");
            Path summaryFilePath = tempDir.resolve(fileName);

            File summaryFile = summaryFilePath.toFile();
            if (summaryFile.exists()) {
                Files.delete(summaryFilePath);
                logger.warn("Existing summary file deleted: {}", summaryFilePath);
            }

            objectMapper.writeValue(summaryFile, payload);
            logger.info("✅ Summary JSON written at: {}", summaryFilePath);

            return summaryFilePath.toAbsolutePath().toString();

        } catch (Exception e) {
            logger.error("❌ Failed to write summary.json", e);
            throw new RuntimeException("Failed to write summary JSON", e);
        }
    }

    public static SummaryPayload buildPayload(KafkaMessage message,
                                              List<SummaryProcessedFile> processedFiles,
                                              List<PrintFile> printFiles,
                                              String mobstatTriggerPath,
                                              int customersProcessed) {

        SummaryPayload payload = new SummaryPayload();

        payload.setBatchID(message.getBatchId());
        payload.setFileName(message.getBatchId() + ".csv");
        payload.setMobstatTriggerFile(mobstatTriggerPath);

        // Header block
        Header header = new Header();
        header.setTenantCode(message.getTenantCode());
        header.setChannelID(message.getChannelID());
        header.setAudienceID(message.getAudienceID());
        header.setSourceSystem(message.getSourceSystem());
        header.setProduct(message.getProduct());
        header.setJobName(message.getJobName());

        // 👇 Don't set root-level timestamp here; will be set in KafkaListenerService
        header.setTimestamp(Instant.now().toString());
        payload.setHeader(header);

        // Determine overall status
        String overallStatus = "Completed";
        if (processedFiles != null && !processedFiles.isEmpty()) {
            boolean allFailed = processedFiles.stream().allMatch(f -> "FAILURE".equalsIgnoreCase(f.getStatusCode()));
            boolean anyFailed = processedFiles.stream().anyMatch(f -> "FAILURE".equalsIgnoreCase(f.getStatusCode()) || "PARTIAL".equalsIgnoreCase(f.getStatusCode()));

            if (allFailed) overallStatus = "Failure";
            else if (anyFailed) overallStatus = "Partial";
        }

        // Metadata block
        Metadata metadata = new Metadata();
        metadata.setTotalFilesProcessed(customersProcessed);
        metadata.setProcessingStatus(overallStatus);
        metadata.setEventOutcomeCode("0");
        metadata.setEventOutcomeDescription("Success");
        payload.setMetadata(metadata);

        // Payload block
        Payload payloadDetails = new Payload();
        payloadDetails.setUniqueConsumerRef(message.getUniqueConsumerRef());
        payloadDetails.setUniqueECPBatchRef(message.getUniqueECPBatchRef());
        payloadDetails.setRunPriority(message.getRunPriority());
        payloadDetails.setEventID(message.getEventID());
        payloadDetails.setEventType(message.getEventType());
        payloadDetails.setRestartKey(message.getRestartKey());
        payloadDetails.setFileCount(processedFiles != null ? processedFiles.size() : 0);
        payload.setPayload(payloadDetails);

        // 👇 Optional: include processed and print files in summary.json
        payload.setProcessedFiles(processedFiles != null ? processedFiles : new ArrayList<>());
        payload.setPrintFiles(printFiles != null ? printFiles : new ArrayList<>());

        return payload;
    }
}

// ✅ KafkaListenerService with All Updated Methods (Final)
package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.*;
import com.nedbank.kafka.filemanage.utils.SummaryJsonWriter;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.*;
import org.springframework.http.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.w3c.dom.*;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

@Service
public class KafkaListenerService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    @Value("${mount.path}")
    private String mountPath;

    @Value("${kafka.topic.output}")
    private String kafkaOutputTopic;

    @Value("${rpt.max.wait.seconds}")
    private int rptMaxWaitSeconds;

    @Value("${rpt.poll.interval.millis}")
    private int rptPollIntervalMillis;

    @Value("${ot.orchestration.api.url}")
    private String otOrchestrationApiUrl;

    @Value("${ot.service.mfc.url}")
    private String orchestrationMfcUrl;

    @Value("${ot.auth.token}")
    private String orchestrationAuthToken;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final BlobStorageService blobStorageService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RestTemplate restTemplate = new RestTemplate();
    private final ExecutorService executor = Executors.newFixedThreadPool(5);

    @Autowired
    public KafkaListenerService(BlobStorageService blobStorageService, KafkaTemplate<String, String> kafkaTemplate) {
        this.blobStorageService = blobStorageService;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "${kafka.topic.input}", groupId = "${kafka.consumer.group.id}")
    public void onKafkaMessage(String rawMessage, Acknowledgment ack) {
        try {
            logger.info("📥 Received Kafka message: {}", rawMessage);
            KafkaMessage message = objectMapper.readValue(rawMessage, KafkaMessage.class);
            String batchId = message.getBatchId();
            Path batchDir = Paths.get(mountPath, "input", message.getSourceSystem(), batchId);
            Files.createDirectories(batchDir);

            for (BatchFile file : message.getBatchFiles()) {
                String blobUrl = file.getBlobUrl();
                String content = blobStorageService.downloadFileContent(blobUrl);
                Path localPath = batchDir.resolve(file.getFilename());
                Files.write(localPath, content.getBytes(StandardCharsets.UTF_8));
                file.setBlobUrl(localPath.toString());
            }

            String url = switch (message.getSourceSystem().toUpperCase()) {
                case "DEBTMAN" -> otOrchestrationApiUrl;
                case "MFC" -> orchestrationMfcUrl;
                default -> throw new IllegalArgumentException("Unsupported source system: " + message.getSourceSystem());
            };

            OTResponse otResponse = callOrchestrationBatchApi(orchestrationAuthToken, url, message);

            if (otResponse == null) {
                kafkaTemplate.send(kafkaOutputTopic, "{\"status\":\"FAILURE\",\"message\":\"OT call failed\"}");
                ack.acknowledge();
                return;
            }

            kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(Map.of(
                    "batchID", batchId,
                    "status", "PENDING",
                    "message", "OT Request Sent"
            )));
            ack.acknowledge();
            executor.submit(() -> processAfterOT(message, otResponse));

        } catch (Exception ex) {
            logger.error("❌ Kafka processing failed", ex);
        }
    }

    private void processAfterOT(KafkaMessage message, OTResponse otResponse) {
        try {
            File xmlFile = waitForXmlFile(otResponse.getJobId(), otResponse.getId());
            if (xmlFile == null) throw new IllegalStateException("XML not found");

            Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(xmlFile);
            doc.getDocumentElement().normalize();

            Map<String, String> accountCustomerMap = extractAccountCustomerMapFromDoc(doc);

            Map<String, SummaryProcessedFile> customerMap = new HashMap<>();
            accountCustomerMap.forEach((acc, cus) -> {
                SummaryProcessedFile spf = new SummaryProcessedFile();
                spf.setAccountNumber(acc);
                spf.setCustomerId(cus);
                customerMap.put(acc, spf);
            });

            Map<String, Map<String, String>> errorMap = parseErrorReport(message);

            Path jobDir = Paths.get(mountPath, "output", message.getSourceSystem(), otResponse.getJobId());
            List<SummaryProcessedFile> processedFiles = buildDetailedProcessedFiles(jobDir, customerMap, errorMap, message);
            List<PrintFile> printFiles = uploadPrintFiles(jobDir, message);

            SummaryPayload payload = SummaryJsonWriter.buildPayload(message, processedFiles, printFiles, null, accountCustomerMap.size());
            payload.setTimestamp(Instant.now().toString());

            String summaryPath = SummaryJsonWriter.writeSummaryJsonToFile(payload);
            String summaryUrl = blobStorageService.uploadSummaryJson(summaryPath, message, "summary_" + message.getBatchId() + ".json");
            payload.setSummaryFileURL(decodeUrl(summaryUrl));

            SummaryResponse response = new SummaryResponse();
            response.setBatchID(message.getBatchId());
            response.setFileName(payload.getFileName());
            response.setHeader(payload.getHeader());
            response.setMetadata(payload.getMetadata());
            response.setSummaryFileURL(payload.getSummaryFileURL());
            response.setTimestamp(payload.getTimestamp());

            kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(
                    new ApiResponse("Summary generated", "COMPLETED", response)));

        } catch (Exception e) {
            logger.error("❌ Error post-OT summary generation", e);
        }
    }

    private Map<String, Map<String, String>> parseErrorReport(KafkaMessage msg) {
        Map<String, Map<String, String>> map = new HashMap<>();
        Path errorPath = Paths.get(mountPath, "output", msg.getSourceSystem(), msg.getJobName(), "ErrorReport.csv");
        if (!Files.exists(errorPath)) return map;
        try (BufferedReader reader = Files.newBufferedReader(errorPath)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\|");
                if (parts.length >= 5) {
                    String acc = parts[0].trim();
                    String method = parts[2].trim().toUpperCase();
                    String status = parts[4].trim();
                    map.computeIfAbsent(acc, k -> new HashMap<>()).put(method, status);
                }
            }
        } catch (Exception e) {
            logger.warn("⚠️ Error reading ErrorReport", e);
        }
        return map;
    }

    private List<SummaryProcessedFile> buildDetailedProcessedFiles(Path jobDir, Map<String, SummaryProcessedFile> customerMap, Map<String, Map<String, String>> errorMap, KafkaMessage msg) throws IOException {
        List<SummaryProcessedFile> result = new ArrayList<>();
        Set<String> folders = Set.of("email", "archive", "html", "mobstat", "txt");

        for (Map.Entry<String, SummaryProcessedFile> entry : customerMap.entrySet()) {
            String account = entry.getKey();
            SummaryProcessedFile spf = entry.getValue();

            for (String folder : folders) {
                Path folderPath = jobDir.resolve(folder);
                if (Files.exists(folderPath)) {
                    Optional<Path> fileOpt = Files.list(folderPath)
                            .filter(p -> p.getFileName().toString().contains(account))
                            .findFirst();

                    if (fileOpt.isPresent()) {
                        Path file = fileOpt.get();
                        String blobUrl = blobStorageService.uploadFile(file.toFile(), msg.getSourceSystem() + "/" + msg.getBatchId() + "/" + folder + "/" + file.getFileName());
                        String decoded = decodeUrl(blobUrl);

                        switch (folder) {
                            case "email" -> {
                                spf.setPdfEmailFileUrl(decoded);
                                spf.setPdfEmailStatus("OK");
                            }
                            case "archive" -> {
                                spf.setPdfArchiveFileUrl(decoded);
                                spf.setPdfArchiveStatus("OK");
                            }
                            case "html" -> {
                                spf.setHtmlEmailFileUrl(decoded);
                                spf.setHtmlEmailStatus("OK");
                            }
                            case "mobstat" -> {
                                spf.setPdfMobstatFileUrl(decoded);
                                spf.setPdfMobstatStatus("OK");
                            }
                            case "txt" -> {
                                spf.setTxtEmailFileUrl(decoded);
                                spf.setTxtEmailStatus("OK");
                            }
                        }
                    } else {
                        Map<String, String> err = errorMap.get(account);
                        if (err != null) {
                            String outputMethod = folder.toUpperCase();
                            String status = err.getOrDefault(outputMethod, "");
                            if (status.equalsIgnoreCase("Failed")) {
                                switch (folder) {
                                    case "email" -> spf.setPdfEmailStatus("Failed");
                                    case "archive" -> spf.setPdfArchiveStatus("Failed");
                                    case "html" -> spf.setHtmlEmailStatus("Failed");
                                    case "mobstat" -> spf.setPdfMobstatStatus("Failed");
                                    case "txt" -> spf.setTxtEmailStatus("Failed");
                                }
                            }
                        }
                    }
                }
            }

            boolean hasFailure = Stream.of(spf.getPdfEmailStatus(), spf.getPdfArchiveStatus(), spf.getHtmlEmailStatus(), spf.getPdfMobstatStatus(), spf.getTxtEmailStatus())
                    .anyMatch(s -> "Failed".equalsIgnoreCase(s));

            boolean allNull = Stream.of(spf.getPdfEmailStatus(), spf.getPdfArchiveStatus(), spf.getHtmlEmailStatus(), spf.getPdfMobstatStatus(), spf.getTxtEmailStatus())
                    .allMatch(Objects::isNull);

            if (allNull) {
                spf.setStatusCode("FAILURE");
                spf.setStatusDescription("No files processed");
            } else if (hasFailure) {
                spf.setStatusCode("PARTIAL");
                spf.setStatusDescription("Some files missing");
            } else {
                spf.setStatusCode("OK");
                spf.setStatusDescription("Success");
            }

            result.add(spf);
        }

        return result;
    }

    private List<PrintFile> uploadPrintFiles(Path jobDir, KafkaMessage msg) {
        List<PrintFile> printFiles = new ArrayList<>();
        Path printDir = jobDir.resolve("print");
        if (!Files.exists(printDir)) return printFiles;
        try (Stream<Path> stream = Files.list(printDir)) {
            stream.filter(Files::isRegularFile).forEach(f -> {
                try {
                    String blob = blobStorageService.uploadFile(f.toFile(), msg.getSourceSystem() + "/print/" + f.getFileName());
                    printFiles.add(new PrintFile(blob));
                } catch (Exception e) {
                    logger.warn("⚠️ Print upload failed", e);
                }
            });
        } catch (IOException ignored) {}
        return printFiles;
    }

    private String decodeUrl(String url) {
        try {
            return URLDecoder.decode(url, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return url;
        }
    }

    @PreDestroy
    public void shutdownExecutor() {
        executor.shutdown();
    }

    static class OTResponse {
        private String jobId;
        private String id;
        public String getJobId() { return jobId; }
        public void setJobId(String jobId) { this.jobId = jobId; }
        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
    }
}
=================================
// ✅ KafkaListenerService with All Updated Methods (Final)
package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.*;
import com.nedbank.kafka.filemanage.utils.SummaryJsonWriter;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.*;
import org.springframework.http.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;
import org.w3c.dom.*;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;

@Service
public class KafkaListenerService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    @Value("${mount.path}")
    private String mountPath;

    @Value("${kafka.topic.output}")
    private String kafkaOutputTopic;

    @Value("${rpt.max.wait.seconds}")
    private int rptMaxWaitSeconds;

    @Value("${rpt.poll.interval.millis}")
    private int rptPollIntervalMillis;

    @Value("${ot.orchestration.api.url}")
    private String otOrchestrationApiUrl;

    @Value("${ot.service.mfc.url}")
    private String orchestrationMfcUrl;

    @Value("${ot.auth.token}")
    private String orchestrationAuthToken;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final BlobStorageService blobStorageService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RestTemplate restTemplate = new RestTemplate();
    private final ExecutorService executor = Executors.newFixedThreadPool(5);

    @Autowired
    public KafkaListenerService(BlobStorageService blobStorageService, KafkaTemplate<String, String> kafkaTemplate) {
        this.blobStorageService = blobStorageService;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "${kafka.topic.input}", groupId = "${kafka.consumer.group.id}")
    public void onKafkaMessage(String rawMessage, Acknowledgment ack) {
        try {
            logger.info("📥 Received Kafka message: {}", rawMessage);
            KafkaMessage message = objectMapper.readValue(rawMessage, KafkaMessage.class);
            String batchId = message.getBatchId();
            Path batchDir = Paths.get(mountPath, "input", message.getSourceSystem(), batchId);
            Files.createDirectories(batchDir);

            for (BatchFile file : message.getBatchFiles()) {
                String blobUrl = file.getBlobUrl();
                String content = blobStorageService.downloadFileContent(blobUrl);
                Path localPath = batchDir.resolve(file.getFilename());
                Files.write(localPath, content.getBytes(StandardCharsets.UTF_8));
                file.setBlobUrl(localPath.toString());
            }

            String url = switch (message.getSourceSystem().toUpperCase()) {
                case "DEBTMAN" -> otOrchestrationApiUrl;
                case "MFC" -> orchestrationMfcUrl;
                default -> throw new IllegalArgumentException("Unsupported source system: " + message.getSourceSystem());
            };

            OTResponse otResponse = callOrchestrationBatchApi(orchestrationAuthToken, url, message);

            if (otResponse == null) {
                kafkaTemplate.send(kafkaOutputTopic, "{\"status\":\"FAILURE\",\"message\":\"OT call failed\"}");
                ack.acknowledge();
                return;
            }

            kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(Map.of(
                    "batchID", batchId,
                    "status", "PENDING",
                    "message", "OT Request Sent"
            )));
            ack.acknowledge();
            executor.submit(() -> processAfterOT(message, otResponse));

        } catch (Exception ex) {
            logger.error("❌ Kafka processing failed", ex);
        }
    }

    private void processAfterOT(KafkaMessage message, OTResponse otResponse) {
        try {
            File xmlFile = waitForXmlFile(otResponse.getJobId(), otResponse.getId());
            if (xmlFile == null) throw new IllegalStateException("XML not found");

            Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(xmlFile);
            doc.getDocumentElement().normalize();

            Map<String, String> accountCustomerMap = extractAccountCustomerMapFromDoc(doc);

            Map<String, SummaryProcessedFile> customerMap = new HashMap<>();
            accountCustomerMap.forEach((acc, cus) -> {
                SummaryProcessedFile spf = new SummaryProcessedFile();
                spf.setAccountNumber(acc);
                spf.setCustomerId(cus);
                customerMap.put(acc, spf);
            });

            Map<String, Map<String, String>> errorMap = parseErrorReport(message);

            Path jobDir = Paths.get(mountPath, "output", message.getSourceSystem(), otResponse.getJobId());
            List<SummaryProcessedFile> processedFiles = buildDetailedProcessedFiles(jobDir, customerMap, errorMap, message);
            List<PrintFile> printFiles = uploadPrintFiles(jobDir, message);

            SummaryPayload payload = SummaryJsonWriter.buildPayload(message, processedFiles, printFiles, null, accountCustomerMap.size());
            payload.setTimestamp(Instant.now().toString());

            String summaryPath = SummaryJsonWriter.writeSummaryJsonToFile(payload);
            String summaryUrl = blobStorageService.uploadSummaryJson(summaryPath, message, "summary_" + message.getBatchId() + ".json");
            payload.setSummaryFileURL(decodeUrl(summaryUrl));

            SummaryResponse response = new SummaryResponse();
            response.setBatchID(message.getBatchId());
            response.setFileName(payload.getFileName());
            response.setHeader(payload.getHeader());
            response.setMetadata(payload.getMetadata());
            response.setSummaryFileURL(payload.getSummaryFileURL());
            response.setTimestamp(payload.getTimestamp());

            kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(
                    new ApiResponse("Summary generated", "COMPLETED", response)));

        } catch (Exception e) {
            logger.error("❌ Error post-OT summary generation", e);
        }
    }

    private Map<String, Map<String, String>> parseErrorReport(KafkaMessage msg) {
        Map<String, Map<String, String>> map = new HashMap<>();
        Path errorPath = Paths.get(mountPath, "output", msg.getSourceSystem(), msg.getJobName(), "ErrorReport.csv");
        if (!Files.exists(errorPath)) return map;
        try (BufferedReader reader = Files.newBufferedReader(errorPath)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\|");
                if (parts.length >= 5) {
                    String acc = parts[0].trim();
                    String method = parts[2].trim().toUpperCase();
                    String status = parts[4].trim();
                    map.computeIfAbsent(acc, k -> new HashMap<>()).put(method, status);
                }
            }
        } catch (Exception e) {
            logger.warn("⚠️ Error reading ErrorReport", e);
        }
        return map;
    }

    private List<SummaryProcessedFile> buildDetailedProcessedFiles(Path jobDir, Map<String, SummaryProcessedFile> customerMap, Map<String, Map<String, String>> errorMap, KafkaMessage msg) throws IOException {
        List<SummaryProcessedFile> result = new ArrayList<>();
        Set<String> folders = Set.of("email", "archive", "html", "mobstat", "txt");

        for (Map.Entry<String, SummaryProcessedFile> entry : customerMap.entrySet()) {
            String account = entry.getKey();
            SummaryProcessedFile spf = entry.getValue();

            for (String folder : folders) {
                Path folderPath = jobDir.resolve(folder);
                if (Files.exists(folderPath)) {
                    Optional<Path> fileOpt = Files.list(folderPath)
                            .filter(p -> p.getFileName().toString().contains(account))
                            .findFirst();

                    if (fileOpt.isPresent()) {
                        Path file = fileOpt.get();
                        String blobUrl = blobStorageService.uploadFile(file.toFile(), msg.getSourceSystem() + "/" + msg.getBatchId() + "/" + folder + "/" + file.getFileName());
                        String decoded = decodeUrl(blobUrl);

                        switch (folder) {
                            case "email" -> {
                                spf.setPdfEmailFileUrl(decoded);
                                spf.setPdfEmailStatus("OK");
                            }
                            case "archive" -> {
                                spf.setPdfArchiveFileUrl(decoded);
                                spf.setPdfArchiveStatus("OK");
                            }
                            case "html" -> {
                                spf.setHtmlEmailFileUrl(decoded);
                                spf.setHtmlEmailStatus("OK");
                            }
                            case "mobstat" -> {
                                spf.setPdfMobstatFileUrl(decoded);
                                spf.setPdfMobstatStatus("OK");
                            }
                            case "txt" -> {
                                spf.setTxtEmailFileUrl(decoded);
                                spf.setTxtEmailStatus("OK");
                            }
                        }
                    } else {
                        Map<String, String> err = errorMap.get(account);
                        if (err != null) {
                            String outputMethod = folder.toUpperCase();
                            String status = err.getOrDefault(outputMethod, "");
                            if (status.equalsIgnoreCase("Failed")) {
                                switch (folder) {
                                    case "email" -> spf.setPdfEmailStatus("Failed");
                                    case "archive" -> spf.setPdfArchiveStatus("Failed");
                                    case "html" -> spf.setHtmlEmailStatus("Failed");
                                    case "mobstat" -> spf.setPdfMobstatStatus("Failed");
                                    case "txt" -> spf.setTxtEmailStatus("Failed");
                                }
                            }
                        }
                    }
                }
            }

            boolean hasFailure = Stream.of(spf.getPdfEmailStatus(), spf.getPdfArchiveStatus(), spf.getHtmlEmailStatus(), spf.getPdfMobstatStatus(), spf.getTxtEmailStatus())
                    .anyMatch(s -> "Failed".equalsIgnoreCase(s));

            boolean allNull = Stream.of(spf.getPdfEmailStatus(), spf.getPdfArchiveStatus(), spf.getHtmlEmailStatus(), spf.getPdfMobstatStatus(), spf.getTxtEmailStatus())
                    .allMatch(Objects::isNull);

            if (allNull) {
                spf.setStatusCode("FAILURE");
                spf.setStatusDescription("No files processed");
            } else if (hasFailure) {
                spf.setStatusCode("PARTIAL");
                spf.setStatusDescription("Some files missing");
            } else {
                spf.setStatusCode("OK");
                spf.setStatusDescription("Success");
            }

            result.add(spf);
        }

        return result;
    }

    private List<PrintFile> uploadPrintFiles(Path jobDir, KafkaMessage msg) {
        List<PrintFile> printFiles = new ArrayList<>();
        Path printDir = jobDir.resolve("print");
        if (!Files.exists(printDir)) return printFiles;
        try (Stream<Path> stream = Files.list(printDir)) {
            stream.filter(Files::isRegularFile).forEach(f -> {
                try {
                    String blob = blobStorageService.uploadFile(f.toFile(), msg.getSourceSystem() + "/print/" + f.getFileName());
                    printFiles.add(new PrintFile(blob));
                } catch (Exception e) {
                    logger.warn("⚠️ Print upload failed", e);
                }
            });
        } catch (IOException ignored) {}
        return printFiles;
    }

    private String decodeUrl(String url) {
        try {
            return URLDecoder.decode(url, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return url;
        }
    }

    @PreDestroy
    public void shutdownExecutor() {
        executor.shutdown();
    }

    static class OTResponse {
        private String jobId;
        private String id;
        public String getJobId() { return jobId; }
        public void setJobId(String jobId) { this.jobId = jobId; }
        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
    }
}

