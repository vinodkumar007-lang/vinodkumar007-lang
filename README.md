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
import java.time.Instant;
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
            String batchId = message.getBatchId();
            String guiRef = message.getUniqueConsumerRef();
            Path jobDir = Paths.get(mountPath, batchId, guiRef);
            Files.createDirectories(jobDir);

            for (BatchFile file : message.getBatchFiles()) {
                String blobUrl = file.getBlobUrl();
                String fileName = extractFileName(blobUrl);
                String content = blobStorageService.downloadFileContent(blobUrl);
                Path localPath = jobDir.resolve(fileName);
                Files.write(localPath, content.getBytes(StandardCharsets.UTF_8));
                file.setBlobUrl(localPath.toString());
                logger.info("✅ Mounted file to: {}", localPath);
            }

            writeAndUploadMetadataJson(message, jobDir);

            String token = fetchAccessToken();
            sendToOpenText(token, message);

            File rptFile = waitForRptFile(jobDir);
            if (rptFile == null) {
                logger.error("⏰ Timeout waiting for .rpt file in {}", jobDir);
                return new ApiResponse("Timeout waiting for .rpt", "error", null);
            }

            logger.info("📄 Found .rpt file: {}", rptFile.getAbsolutePath());

            Map<String, String> accountCustomerMap = extractAccountCustomerMapFromRpt(rptFile);

            List<SummaryProcessedFile> processedFiles = new ArrayList<>();
            String[] subDirs = {"archive", "email", "mobstat", "mobstat_trigger", "print"};
            for (String sub : subDirs) {
                Path subFolder = jobDir.resolve("output").resolve(sub);
                if (!Files.exists(subFolder)) continue;
                File[] pdfs = subFolder.toFile().listFiles(f -> f.getName().endsWith(".pdf"));
                if (pdfs == null) continue;
                for (File f : pdfs) {
                    String accountNumber = extractAccountNumberFromFile(f.getName());
                    String customerNumber = accountCustomerMap.get(accountNumber);
                    if (accountNumber != null && customerNumber != null) {
                        String blobPath = String.format("output/%s/%s/%s", batchId, sub, f.getName());
                        String uploadedUrl = blobStorageService.uploadFile(f.getAbsolutePath(), blobPath);
                        SummaryProcessedFile pf = new SummaryProcessedFile();
                        pf.setAccountNumber(accountNumber);
                        pf.setCustomerId(customerNumber);
                        pf.setBlobURL(decodeUrl(uploadedUrl));
                        pf.setStatusCode("OK");
                        pf.setStatusDescription("Uploaded");
                        processedFiles.add(pf);
                        FileUtils.forceDelete(f);
                    }
                }
            }

            SummaryPayload payload = SummaryJsonWriter.buildPayload(message, processedFiles);
            String summaryPath = SummaryJsonWriter.writeSummaryJsonToFile(payload);
            String summaryUrl = blobStorageService.uploadSummaryJson(summaryPath, message, "summary_" + batchId + ".json");
            payload.setSummaryFileURL(decodeUrl(summaryUrl));

            SummaryResponse summaryResponse = new SummaryResponse(payload);
            return new ApiResponse("Success", "success", summaryResponse);
        } catch (Exception ex) {
            logger.error("❌ Failed in processing", ex);
            return new ApiResponse("Processing failed: " + ex.getMessage(), "error", null);
        }
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
                    if (accMatch.find()) {
                        currentAccount = accMatch.group(1);
                    }
                }
                if (line.contains("DM_CISNumber")) {
                    Matcher custMatch = Pattern.compile("DM_CISNumber\\s+(\\d{6,})").matcher(line);
                    if (custMatch.find()) {
                        currentCustomer = custMatch.group(1);
                    }
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

    private String extractAccountNumberFromFile(String fileName) {
        try {
            Matcher m = Pattern.compile("(\\d{9,})_").matcher(fileName);
            return m.find() ? m.group(1) : null;
        } catch (Exception e) {
            return null;
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
            logger.info("✅ metadata.json uploaded to {}", blobPath);
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
            logger.error("❌ Failed to get OTDS token", e);
            throw new RuntimeException("OTDS auth failed");
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
            logger.info("📤 Sent metadata to OT");
        } catch (Exception ex) {
            logger.error("❌ Failed to send data to OT", ex);
            throw new RuntimeException("OT call failed");
        }
    }

    private File waitForRptFile(Path jobDir) {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < rptMaxWaitSeconds * 1000L) {
            File[] rptFiles = jobDir.toFile().listFiles(f -> f.getName().endsWith(".rpt"));
            if (rptFiles != null && rptFiles.length > 0) {
                return rptFiles[0];
            }
            try {
                TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
            } catch (InterruptedException ignored) {}
        }
        return null;
    }

    private String extractFileName(String url) {
        try {
            String decoded = URLDecoder.decode(url, StandardCharsets.UTF_8.name());
            return Paths.get(new URI(decoded).getPath()).getFileName().toString();
        } catch (Exception e) {
            logger.warn("⚠️ Failed to extract file name, fallback to last segment: {}", url);
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
