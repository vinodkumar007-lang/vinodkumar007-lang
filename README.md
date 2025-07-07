package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.*;
import com.nedbank.kafka.filemanage.utils.SummaryJsonWriter;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.*;
import org.springframework.http.*;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.*;

@Service
public class KafkaListenerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    @Value("${mount.path}")
    private String mountPath;

    @Value("${opentext.api.url}")
    private String opentextApiUrl;

    @Value("${otds.token.url}")
    private String otdsTokenUrl;

    @Value("${otds.username}")
    private String otdsUsername;

    @Value("${otds.password}")
    private String otdsPassword;

    @Value("${otds.client-id}")
    private String otdsClientId;

    @Value("${otds.client-secret}")
    private String otdsClientSecret;

    @Value("${kafka.topic.output}")
    private String kafkaOutputTopic;

    @Value("${rpt.max.wait.seconds}")
    private int rptMaxWaitSeconds;

    @Value("${rpt.poll.interval.millis}")
    private int rptPollIntervalMillis;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final BlobStorageService blobStorageService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RestTemplate restTemplate = new RestTemplate();

    @Autowired
    public KafkaListenerService(BlobStorageService blobStorageService,
                                KafkaTemplate<String, String> kafkaTemplate) {
        this.blobStorageService = blobStorageService;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "${kafka.topic.input}", groupId = "${kafka.consumer.group.id}",
            containerFactory = "kafkaListenerContainerFactory")
    public void consumeKafkaMessage(String message) {
        try {
            logger.info("📩 Received Kafka message.");
            KafkaMessage kafkaMessage = objectMapper.readValue(message, KafkaMessage.class);
            ApiResponse response = processSingleMessage(kafkaMessage);
            kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(response));
            logger.info("✅ Sent processed response to Kafka output topic.");
        } catch (Exception ex) {
            logger.error("❌ Error processing Kafka message", ex);
        }
    }

    private ApiResponse processSingleMessage(KafkaMessage message) {
        try {
            List<BatchFile> batchFiles = message.getBatchFiles();
            if (batchFiles == null || batchFiles.isEmpty()) {
                return new ApiResponse("No batch files found", "error", null);
            }

            long dataCount = batchFiles.stream().filter(f -> "DATA".equalsIgnoreCase(f.getFileType())).count();
            long refCount = batchFiles.stream().filter(f -> "REF".equalsIgnoreCase(f.getFileType())).count();
            boolean hasInvalid = batchFiles.stream().anyMatch(f -> f.getFileType() == null || f.getFileType().trim().isEmpty());

            if (hasInvalid) return new ApiResponse("Invalid or empty fileType found", "error", null);
            if (dataCount == 0) return new ApiResponse("No DATA files to process", "error", null);
            if (dataCount > 1 && refCount == 0) return new ApiResponse("Too many DATA files without REF", "error", null);

            List<BatchFile> dataFilesOnly = batchFiles.stream().filter(f -> "DATA".equalsIgnoreCase(f.getFileType())).toList();
            message.setBatchFiles(dataFilesOnly);

            String batchId = message.getBatchId();
            String guiRef = message.getUniqueConsumerRef();
            Path jobDir = Paths.get(mountPath, batchId, guiRef);
            Files.createDirectories(jobDir);

            for (BatchFile file : message.getBatchFiles()) {
                String blobUrl = file.getBlobUrl();
                String content = blobStorageService.downloadFileContent(blobUrl);
                Path localPath = jobDir.resolve(extractFileName(blobUrl));
                Files.write(localPath, content.getBytes(StandardCharsets.UTF_8));
                file.setBlobUrl(localPath.toString());
            }

            writeAndUploadMetadataJson(message, jobDir);

            String token = fetchAccessToken();
            sendToOpenText(token, message);

            logger.info("⏳ Waiting for .rpt file every {}ms, up to {}s...", rptPollIntervalMillis, rptMaxWaitSeconds);
            File rptFile = waitForRptFile(jobDir);
            if (rptFile == null) return new ApiResponse("Timeout waiting for .rpt", "error", null);

            Map<String, String> accountCustomerMap = extractAccountCustomerMapFromRpt(rptFile);

            SummaryPayload payload = SummaryJsonWriter.buildPayload(message, jobDir, blobStorageService, accountCustomerMap);
            String summaryPath = SummaryJsonWriter.writeSummaryJsonToFile(payload);
            String summaryUrl = blobStorageService.uploadSummaryJson(summaryPath, message, "summary_" + batchId + ".json");
            payload.setSummaryFileURL(decodeUrl(summaryUrl));

            return new ApiResponse("Success", "success", new SummaryResponse(payload));
        } catch (Exception ex) {
            logger.error("❌ Failed in processing", ex);
            return new ApiResponse("Processing failed: " + ex.getMessage(), "error", null);
        }
    }

    private void writeAndUploadMetadataJson(KafkaMessage message, Path jobDir) {
        try {
            Map<String, Object> metaMap = objectMapper.convertValue(message, Map.class);
            if (metaMap.containsKey("batchFiles")) {
                List<Map<String, Object>> files = (List<Map<String, Object>>) metaMap.get("batchFiles");
                for (Map<String, Object> f : files) {
                    Object blob = f.remove("blobUrl");
                    if (blob != null) f.put("fileLocation", blob);
                }
            }
            String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(metaMap);
            File metaFile = new File(jobDir.toFile(), "metadata.json");
            FileUtils.writeStringToFile(metaFile, json, StandardCharsets.UTF_8);
            String blobPath = String.format("%s/Trigger/metadata_%s.json", message.getSourceSystem(), message.getBatchId());
            blobStorageService.uploadFile(metaFile.getAbsolutePath(), blobPath);
            FileUtils.forceDelete(metaFile);
        } catch (Exception ex) {
            logger.error("❌ metadata.json generation failed", ex);
        }
    }

    private String fetchAccessToken() {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_FORM_URLENCODED);
            MultiValueMap<String, String> body = new LinkedMultiValueMap<>();
            body.add("grant_type", "password");
            body.add("username", otdsUsername);
            body.add("password", otdsPassword);
            body.add("client_id", otdsClientId);
            body.add("client_secret", otdsClientSecret);

            HttpEntity<MultiValueMap<String, String>> request = new HttpEntity<>(body, headers);
            ResponseEntity<Map> response = restTemplate.postForEntity(otdsTokenUrl, request, Map.class);
            return (String) response.getBody().get("access_token");
        } catch (Exception e) {
            throw new RuntimeException("OTDS auth failed", e);
        }
    }

    private void sendToOpenText(String token, KafkaMessage msg) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.set("Authorization", "Bearer " + token);
            headers.setContentType(MediaType.APPLICATION_JSON);
            String json = objectMapper.writeValueAsString(msg);
            HttpEntity<String> request = new HttpEntity<>(json, headers);
            restTemplate.postForEntity(opentextApiUrl, request, String.class);
        } catch (Exception ex) {
            throw new RuntimeException("OT call failed", ex);
        }
    }

    private File waitForRptFile(Path jobDir) {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < rptMaxWaitSeconds * 1000L) {
            File[] rptFiles = jobDir.toFile().listFiles(f -> f.getName().endsWith(".rpt"));
            if (rptFiles != null && rptFiles.length > 0) return rptFiles[0];
            try {
                TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
            } catch (InterruptedException ignored) {}
        }
        return null;
    }

    private Map<String, String> extractAccountCustomerMapFromRpt(File rptFile) throws IOException {
        Map<String, String> accountCustomerMap = new HashMap<>();
        String currentAccount = null;
        String currentCustomer = null;

        try (BufferedReader reader = new BufferedReader(new FileReader(rptFile))) {
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.contains("DM_AccountNumber")) {
                    Matcher accMatch = Pattern.compile("DM_AccountNumber\\s+(\\d{9,})").matcher(line);
                    if (accMatch.find()) currentAccount = accMatch.group(1);
                }
                if (line.contains("DM_CISNumber")) {
                    Matcher custMatch = Pattern.compile("DM_CISNumber\\s+(\\d{6,})").matcher(line);
                    if (custMatch.find()) currentCustomer = custMatch.group(1);
                }
                if (currentAccount != null && currentCustomer != null) {
                    accountCustomerMap.put(currentAccount, currentCustomer);
                    currentAccount = null;
                    currentCustomer = null;
                }
            }
        }
        return accountCustomerMap;
    }

    private String extractFileName(String url) {
        try {
            String decoded = URLDecoder.decode(url, StandardCharsets.UTF_8.name());
            return Paths.get(new URI(decoded).getPath()).getFileName().toString();
        } catch (Exception e) {
            String[] parts = url.split("/");
            return parts[parts.length - 1];
        }
    }

    private String decodeUrl(String encodedUrl) {
        try {
            return URLDecoder.decode(encodedUrl, StandardCharsets.UTF_8.name());
        } catch (Exception e) {
            return encodedUrl;
        }
    }
}

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
            logger.error("❌ SummaryPayload is null. Cannot write summary.json.");
            throw new IllegalArgumentException("SummaryPayload cannot be null");
        }

        try {
            String batchId = payload.getBatchID() != null ? payload.getBatchID() : "unknown";
            String fileName = "summary_" + batchId + ".json";

            Path tempDir = Files.createTempDirectory("summaryFiles");
            Path summaryFilePath = tempDir.resolve(fileName);

            File summaryFile = summaryFilePath.toFile();
            if (summaryFile.exists()) {
                Files.delete(summaryFilePath);
                logger.warn("⚠️ Existing summary file deleted: {}", summaryFilePath);
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
                                              List<SummaryPrintFile> printFiles,
                                              String mobstatTriggerPath) {

        SummaryPayload payload = new SummaryPayload();

        payload.setBatchID(message.getBatchId());
        payload.setFileName(message.getBatchId() + ".csv");
        payload.setMobstatTriggerFile(mobstatTriggerPath);

        SummaryHeader header = new SummaryHeader();
        header.setTenantCode(message.getTenantCode());
        header.setChannelID(message.getChannelID());
        header.setAudienceID(message.getAudienceID());
        header.setSourceSystem(message.getSourceSystem());
        header.setProduct(message.getProduct());
        header.setJobName(message.getJobName());
        header.setTimestamp(Instant.now().toString());
        payload.setHeader(header);

        SummaryMetadata metadata = new SummaryMetadata();
        metadata.setTotalFilesProcessed(processedFiles != null ? processedFiles.size() : 0);
        metadata.setProcessingStatus("Completed");
        metadata.setEventOutcomeCode("0");
        metadata.setEventOutcomeDescription("Success");
        payload.setMetadata(metadata);

        SummaryPayloadDetails payloadDetails = new SummaryPayloadDetails();
        payloadDetails.setUniqueConsumerRef(message.getUniqueConsumerRef());
        payloadDetails.setUniqueECPBatchRef(message.getUniqueECPBatchRef());
        payloadDetails.setRunPriority(message.getRunPriority());
        payloadDetails.setEventID(message.getEventID());
        payloadDetails.setEventType(message.getEventType());
        payloadDetails.setRestartKey(message.getRestartKey());
        payloadDetails.setFileCount(processedFiles != null ? processedFiles.size() : 0);
        payload.setPayload(payloadDetails);

        payload.setProcessedFiles(processedFiles);
        payload.setPrintFiles(printFiles);

        return payload;
    }
}
