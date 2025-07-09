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
            logger.info("\uD83D\uDCE9 Received Kafka message.");
            KafkaMessage kafkaMessage = objectMapper.readValue(message, KafkaMessage.class);
            ApiResponse response = processSingleMessage(kafkaMessage);
            kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(response));
            logger.info("\u2705 Sent processed response to Kafka output topic.");
        } catch (Exception ex) {
            logger.error("\u274C Error processing Kafka message", ex);
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

            logger.info("⏳ Waiting for XML file (*.xml) every {}ms, up to {}s...", rptPollIntervalMillis, rptMaxWaitSeconds);
            File xmlFile = waitForAnyXmlFile(jobDir);
            if (xmlFile == null) return new ApiResponse("Timeout waiting for XML file", "error", null);

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
            logger.error("\u274C Failed in processing", ex);
            return new ApiResponse("Processing failed: " + ex.getMessage(), "error", null);
        }
    }

    private File waitForAnyXmlFile(Path jobDir) {
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

    // ... other unchanged methods remain as is
}
