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
import org.w3c.dom.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
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
            Path batchDir = Paths.get(mountPath, "output", message.getSourceSystem(), batchId);
            Files.createDirectories(batchDir);

            for (BatchFile file : message.getBatchFiles()) {
                String blobUrl = file.getBlobUrl();
                String content = blobStorageService.downloadFileContent(blobUrl);
                Path localPath = batchDir.resolve(extractFileName(blobUrl));
                Files.write(localPath, content.getBytes(StandardCharsets.UTF_8));
                file.setBlobUrl(localPath.toString());
            }

            writeAndUploadMetadataJson(message, batchDir);

            String token = fetchAccessToken();
            String jobId = sendToOpenText(token, message);

            if (jobId == null || jobId.isBlank()) return new ApiResponse("Missing jobId from OT response", "error", null);

            Path jobDir = Paths.get(mountPath, "output", message.getSourceSystem(), jobId);

            logger.info("⏳ Waiting for .xml output from OT in: {}", jobDir);
            File xmlFile = waitForXmlFile(jobDir);
            if (xmlFile == null) return new ApiResponse("Timeout waiting for XML", "error", null);

            Map<String, String> accountCustomerMap = extractAccountCustomerMapFromXml(xmlFile);

            List<SummaryProcessedFile> processedFiles = buildAndUploadProcessedFiles(jobDir, accountCustomerMap, message);
            List<PrintFile> printFiles = buildAndUploadPrintFiles(jobDir, message);
            String mobstatTriggerPath = jobDir.resolve("mobstat_trigger/DropData.trigger").toString();

            SummaryPayload payload = SummaryJsonWriter.buildPayload(message, processedFiles, printFiles, mobstatTriggerPath);
            String summaryPath = SummaryJsonWriter.writeSummaryJsonToFile(payload);
            String summaryUrl = blobStorageService.uploadSummaryJson(summaryPath, message, "summary_" + batchId + ".json");
            payload.setSummaryFileURL(decodeUrl(summaryUrl));

            return new ApiResponse("Success", "success", new SummaryResponse(payload));
        } catch (Exception ex) {
            logger.error("❌ Failed in processing", ex);
            return new ApiResponse("Processing failed: " + ex.getMessage(), "error", null);
        }
    }

    private File waitForXmlFile(Path jobDir) {
        long start = System.currentTimeMillis();
        while (System.currentTimeMillis() - start < rptMaxWaitSeconds * 1000L) {
            File[] xmlFiles = jobDir.toFile().listFiles(f -> f.getName().toLowerCase().endsWith(".xml"));
            if (xmlFiles != null && xmlFiles.length > 0) return xmlFiles[0];
            try {
                TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
            } catch (InterruptedException ignored) {}
        }
        return null;
    }

    private Map<String, String> extractAccountCustomerMapFromXml(File xmlFile) throws Exception {
        Map<String, String> map = new HashMap<>();
        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document doc = dBuilder.parse(xmlFile);
        doc.getDocumentElement().normalize();

        NodeList customerNodes = doc.getElementsByTagName("customer");
        for (int i = 0; i < customerNodes.getLength(); i++) {
            Element customerElement = (Element) customerNodes.item(i);
            NodeList keys = customerElement.getElementsByTagName("key");

            String account = null;
            String customer = null;

            for (int j = 0; j < keys.getLength(); j++) {
                Element keyElement = (Element) keys.item(j);
                String keyName = keyElement.getAttribute("name");
                String keyValue = keyElement.getTextContent();

                if ("AccountNumber".equalsIgnoreCase(keyName)) {
                    account = keyValue;
                } else if ("CISNumber".equalsIgnoreCase(keyName)) {
                    customer = keyValue;
                }
            }

            if (account != null && customer != null) {
                map.put(account, customer);
            }
        }

        return map;
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

    private String sendToOpenText(String token, KafkaMessage msg) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.set("Authorization", "Bearer " + token);
            headers.setContentType(MediaType.APPLICATION_JSON);
            String json = objectMapper.writeValueAsString(msg);
            HttpEntity<String> request = new HttpEntity<>(json, headers);
            ResponseEntity<Map> response = restTemplate.postForEntity(opentextApiUrl, request, Map.class);
            if (response.getStatusCode().is2xxSuccessful() && response.getBody() != null) {
                Object jobId = response.getBody().get("jobId");
                return jobId != null ? jobId.toString() : null;
            }
            return null;
        } catch (Exception ex) {
            throw new RuntimeException("OT call failed", ex);
        }
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

    private List<PrintFile> buildAndUploadPrintFiles(Path jobDir, KafkaMessage msg) throws IOException {
        List<PrintFile> list = new ArrayList<>();
        Path printDir = jobDir.resolve("print");
        if (!Files.exists(printDir)) return list;

        Files.list(printDir).forEach(file -> {
            try {
                String fileName = file.getFileName().toString();
                String blobUrl = blobStorageService.uploadFile(Files.readString(file), String.format("%s/%s/%s/print/%s",
                        msg.getSourceSystem(), msg.getBatchId(), msg.getUniqueConsumerRef(), fileName));
                PrintFile print = new PrintFile();
                print.setPrintFileURL(blobUrl);
                list.add(print);
            } catch (Exception e) {
                logger.error("Error uploading print file: {}", file, e);
            }
        });
        return list;
    }

    private List<SummaryProcessedFile> buildAndUploadProcessedFiles(Path jobDir, Map<String, String> accountCustomerMap, KafkaMessage msg) throws IOException {
        List<SummaryProcessedFile> list = new ArrayList<>();
        List<String> folderNames = List.of("archive", "email", "html", "mobstat", "txt");

        for (String folder : folderNames) {
            Path subDir = jobDir.resolve(folder);
            if (!Files.exists(subDir)) continue;

            Files.list(subDir).forEach(file -> {
                try {
                    String fileName = file.getFileName().toString();
                    String account = extractAccountFromFileName(fileName);
                    if (account == null) return;
                    String customer = accountCustomerMap.get(account);
                    SummaryProcessedFile entry = list.stream().filter(e -> account.equals(e.getAccountNumber())).findFirst().orElseGet(() -> {
                        SummaryProcessedFile newEntry = new SummaryProcessedFile();
                        newEntry.setAccountNumber(account);
                        newEntry.setCustomerId(customer);
                        newEntry.setStatusCode("OK");
                        newEntry.setStatusDescription("Success");
                        list.add(newEntry);
                        return newEntry;
                    });
                    String blobUrl = blobStorageService.uploadFile(Files.readString(file), String.format("%s/%s/%s/%s/%s",
                            msg.getSourceSystem(), msg.getBatchId(), msg.getUniqueConsumerRef(), folder, fileName));
                    switch (folder) {
                        case "archive" -> entry.setPdfArchiveFileUrl(blobUrl);
                        case "email" -> entry.setPdfEmailFileUrl(blobUrl);
                        case "html" -> entry.setHtmlEmailFileUrl(blobUrl);
                        case "txt" -> entry.setTxtEmailFileUrl(blobUrl);
                        case "mobstat" -> entry.setPdfMobstatFileUrl(blobUrl);
                    }
                } catch (Exception e) {
                    logger.error("Error uploading file: {}", file, e);
                }
            });
        }
        return list;
    }

    private String extractAccountFromFileName(String fileName) {
        Matcher matcher = Pattern.compile("(\\d{9,})").matcher(fileName);
        return matcher.find() ? matcher.group(1) : null;
    }
}
