jobs/<jobId>/<id>/docgen/<single folder>/output/_STDDELIVERYFILE.xml

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
import java.util.stream.Stream;

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
            logger.info("Received Kafka message.");
            KafkaMessage kafkaMessage = objectMapper.readValue(message, KafkaMessage.class);
            ApiResponse response = processSingleMessage(kafkaMessage);
            logger.info("Final Summary JSON: \n{}", objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(response.getSummaryPayload()));
            logger.info("Final API Response: \n{}", objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(response));
            kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(response));
            logger.info("Sent processed response to Kafka output topic.");
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

            List<BatchFile> dataFilesOnly = batchFiles.stream()
                    .filter(f -> "DATA".equalsIgnoreCase(f.getFileType()))
                    .toList();
            if (dataFilesOnly.isEmpty()) return new ApiResponse("No DATA files to process", "error", null);
            message.setBatchFiles(dataFilesOnly);

            String batchId = message.getBatchId();
            Path batchDir = Paths.get(mountPath, "input", message.getSourceSystem(), batchId);
            Files.createDirectories(batchDir);

            for (BatchFile file : message.getBatchFiles()) {
                String blobUrl = file.getBlobUrl();
                String content = blobStorageService.downloadFileContent(blobUrl);
                Path localPath = batchDir.resolve(message.getSourceSystem() + ".csv");
                Files.write(localPath, content.getBytes(StandardCharsets.UTF_8));
                file.setBlobUrl(localPath.toString());
            }

            writeAndUploadMetadataJson(message, batchDir);

            String token = "eyJraWQiOiJjZjkwMjJmMjUxNjM2MjQzNjI5YmE1ZmNmMjMwZDI4YzFlOTJkNDNiIiwidHlwIjoiSldUIiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiIxZGY1MmRlMy1hYTJhLTQwMDUtODBmMi1jYzljMTY5NDU4ZDAiLCJzY3AiOlsib3Rkczpncm91cHMiLCJvdGRzOnJvbGVzIl0sInJvbGUiOltdLCJncnAiOlsidGVuYW50YWRtaW5zQGV4c3RyZWFtLnJvbGUiLCJvdGRzYWRtaW5zQG90ZHMuYWRtaW4iLCJvdGFkbWluc0BvdGRzLmFkbWluIiwiZW1wb3dlcmFkbWluc0BleHN0cmVhbS5yb2xlIl0sImRtcCI6eyJPVERTX0NSRURTX0FVVEgiOiJ0cnVlIiwiT1REU19IQVNfUEFTU1dPUkQiOiJmYWxzZSJ9LCJydGkiOiJhMTdjOTk3My1iYWYwLTQzODItODdlNy02NWNkNzJhOGU4N2UiLCJzYXQiOjE3NTIxNTY0OTQsImlzcyI6Imh0dHBzOi8vZGV2LWV4c3RyZWFtLm5lZG5ldC5jby56YTo0NDMvb3Rkcy9vdGRzd3MiLCJncnQiOiJwYXNzd29yZCIsInN1Yl90eXAiOjAsInR5cCI6ImFjY2Vzc190b2tlbiIsInBpZCI6ImV4c3RyZWFtLnJvbGUiLCJyaWQiOnt9LCJ0aWQiOiJkZXYtZXhzdHJlYW0iLCJzaWQiOiJkYmE5MDRmZS0yM2RmLTQyYzItYjE3Mi1jNjE1MGQzZjM1YjIiLCJ1aWQiOiJ0ZW5hbnRhZG1pbkBleHN0cmVhbS5yb2xlIiwidW5tIjoidGVuYW50YWRtaW4iLCJuYW1lIjoidGVuYW50YWRtaW4iLCJleHAiOjE3ODM2OTI0OTQsImlhdCI6MTc1MjE1NjQ5NCwianRpIjoiMGU4NjFhZmItMTM4ZS00ZWQwLTkyZTUtNmU0OGVlMDdhZGZjIiwiY2lkIjoiZGV2ZXhzdHJlYW1jbGllbnQifQ.hnLu3p81p4lu4R5p7vJDbB4RPUuit3FONsc1brou_QVxyfarmZE4zZNYScNOd0z8npFLZqScxZj_00WmpCNE9sDtMhGav4QH_gHBgXtLrjlYGlEf4STFTcTIiBAdFVX8W0u_nmOHWgPoGZFjDoUwAsGXlOCY-UNXRr0ZCONaPP0DE13-UxYiSywkMy1a108bD0MXaF8yhuT-QrsfBKpRIqjZR8ffTl5iBpRULlVmEk2fbeChz4EbSgaHqAmC-h4CWPT4HFOwfn0r6IzIQsMVYrKmdRxuLdEb7RpeLfnDktkX-9s0ifnp65AW3JcMI7CaIrIyvHTP_AM5t-xZnJ7a5g";
            String jobId = callOrchestrationBatchApi(token, message);
            if (jobId == null) return new ApiResponse("Failed to call OT batch input API", "error", null);

            String instanceId = message.getBatchId();
            logger.info("🪪 OT JobId: {}, InstanceId: {}", jobId, instanceId);

            Path xmlRoot = Paths.get(mountPath, "jobs", jobId, instanceId, "docgen");
            File xmlFile = waitForXmlFile(xmlRoot);
            if (xmlFile == null) return new ApiResponse("_STDDELIVERYFILE.xml not found", "error", null);

            Map<String, String> accountCustomerMap = extractAccountCustomerMapFromXml(xmlFile);

            Path jobDir = Paths.get(mountPath, "output", message.getSourceSystem(), jobId);
            List<SummaryProcessedFile> processedFiles = buildAndUploadProcessedFiles(jobDir, accountCustomerMap, message);

            Map<String, String> successMap = new HashMap<>();
            for (SummaryProcessedFile s : processedFiles) {
                successMap.put(s.getAccountNumber(), s.getCustomerId());
            }
            processedFiles.addAll(appendFailureEntries(jobDir, message, successMap));

            List<PrintFile> printFiles = buildAndUploadPrintFiles(jobDir, message);
            String mobstatTriggerPath = jobDir.resolve("mobstat_trigger/DropData.trigger").toString();

            SummaryPayload payload = SummaryJsonWriter.buildPayload(message, processedFiles, printFiles, mobstatTriggerPath);
            String summaryPath = SummaryJsonWriter.writeSummaryJsonToFile(payload);
            String summaryUrl = blobStorageService.uploadSummaryJson(summaryPath, message, "summary_" + batchId + ".json");
            payload.setSummaryFileURL(decodeUrl(summaryUrl));

            return new ApiResponse("Success", "success", new SummaryResponse(payload));
        } catch (Exception ex) {
            logger.error("Failed in processing", ex);
            return new ApiResponse("Processing failed: " + ex.getMessage(), "error", null);
        }
    }

    private Map<String, String> readFailureReport(Path jobDir) {
        Map<String, String> failureMap = new HashMap<>();
        Path errorReportPath = jobDir.resolve("ErrorReport.csv");
        if (!Files.exists(errorReportPath)) return failureMap;

        try (BufferedReader reader = Files.newBufferedReader(errorReportPath)) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] fields = line.split(",");
                if (fields.length >= 2) {
                    String account = fields[0].trim();
                    String customer = fields[1].trim();
                    failureMap.put(account, customer);
                }
            }
        } catch (IOException e) {
            logger.error("❌ Failed to read ErrorReport.csv", e);
        }
        return failureMap;
    }

    private List<SummaryProcessedFile> appendFailureEntries(Path jobDir, KafkaMessage msg, Map<String, String> successMap) {
        Map<String, String> failureMap = readFailureReport(jobDir);
        List<SummaryProcessedFile> failures = new ArrayList<>();

        for (Map.Entry<String, String> entry : failureMap.entrySet()) {
            String account = entry.getKey();
            String customer = entry.getValue();
            if (!successMap.containsKey(account)) {
                SummaryProcessedFile failEntry = new SummaryProcessedFile();
                failEntry.setAccountNumber(account);
                failEntry.setCustomerId(customer);
                failEntry.setStatusCode("FAILURE");
                failEntry.setStatusDescription("Processing failed");
                failures.add(failEntry);
            }
        }

        return failures;
    }

    private File waitForXmlFile(Path dir) throws InterruptedException, IOException {
        logger.info("Waiting for _STDDELIVERYFILE.xml in: {}", dir);
        long startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < rptMaxWaitSeconds * 1000L) {
            try (Stream<Path> files = Files.walk(dir)) {
                Optional<File> xml = files
                        .filter(Files::isRegularFile)
                        .map(Path::toFile)
                        .filter(file -> file.getName().endsWith(".xml"))
                        .findFirst();

                if (xml.isPresent()) {
                    logger.info("Found XML file: {}", xml.get().getAbsolutePath());
                    return xml.get();
                } else {
                    logger.info("Still waiting for _STDDELIVERYFILE.xml... will retry in {}ms", rptPollIntervalMillis);
                    TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
                }
            }
        }
        logger.error("Timed out after {} seconds waiting for _STDDELIVERYFILE.xml", rptMaxWaitSeconds);
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
                if (blob != null) {
                    f.put("MountPath", blob);  // ✅ Include mount path in metadata
                }
            }
        }

        String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(metaMap);

        // ✅ Log metadata.json before uploading
        logger.info("📝 Metadata JSON before sending to OT:\n{}", json);

        File metaFile = new File(jobDir.toFile(), "metadata.json");
        FileUtils.writeStringToFile(metaFile, json, StandardCharsets.UTF_8);
        String blobPath = String.format("%s/Trigger/metadata_%s.json", message.getSourceSystem(), message.getBatchId());
        blobStorageService.uploadFile(metaFile.getAbsolutePath(), blobPath);
        FileUtils.forceDelete(metaFile);
    } catch (Exception ex) {
        logger.error("metadata.json generation failed", ex);
    }
}

    private String callOrchestrationBatchApi(String token, KafkaMessage msg) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.set("Authorization", "Bearer " + token);
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<String> request = new HttpEntity<>(objectMapper.writeValueAsString(msg), headers);
            ResponseEntity<Map> response = restTemplate.exchange(otOrchestrationApiUrl, HttpMethod.POST, request, Map.class);

            List<Map<String, Object>> data = (List<Map<String, Object>>) response.getBody().get("data");
            if (data != null && !data.isEmpty()) {
                return (String) data.get(0).get("jobId");
            }
        } catch (Exception e) {
            logger.error("Failed OT Orchestration call", e);
        }
        return null;
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

    private String decodeUrl(String encodedUrl) {
        try {
            return URLDecoder.decode(encodedUrl, StandardCharsets.UTF_8.name());
        } catch (Exception e) {
            return encodedUrl;
        }
    }
}
