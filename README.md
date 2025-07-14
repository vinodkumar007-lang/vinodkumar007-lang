package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.*;
import com.nedbank.kafka.filemanage.utils.SummaryJsonWriter;
import jakarta.annotation.PreDestroy;
import org.apache.commons.io.FileUtils;
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

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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
    private final ExecutorService executor = Executors.newFixedThreadPool(5);

    @Autowired
    public KafkaListenerService(BlobStorageService blobStorageService, KafkaTemplate<String, String> kafkaTemplate) {
        this.blobStorageService = blobStorageService;
        this.kafkaTemplate = kafkaTemplate;
    }

    @KafkaListener(topics = "${kafka.topic.input}", groupId = "${kafka.consumer.group.id}", containerFactory = "kafkaListenerContainerFactory")
    public void onKafkaMessage(String rawMessage, Acknowledgment ack) {
        logger.info("📥 Received Kafka message");
        try {
            KafkaMessage message = objectMapper.readValue(rawMessage, KafkaMessage.class);
            logger.info("🔎 Parsed Kafka message with batchId: {}", message.getBatchId());

            List<BatchFile> dataFiles = message.getBatchFiles().stream()
                    .filter(f -> "DATA".equalsIgnoreCase(f.getFileType()))
                    .toList();
            message.setBatchFiles(dataFiles);
            logger.info("📄 Filtered DATA files: {}", dataFiles.size());

            String batchId = message.getBatchId();
            Path batchDir = Paths.get(mountPath, "input", message.getSourceSystem(), batchId);
            Files.createDirectories(batchDir);
            logger.info("📁 Created input directory: {}", batchDir);

            for (BatchFile file : dataFiles) {
                String blobUrl = file.getBlobUrl();
                String content = blobStorageService.downloadFileContent(blobUrl);
                Path localPath = batchDir.resolve(message.getSourceSystem() + ".csv");
                Files.write(localPath, content.getBytes(StandardCharsets.UTF_8));
                file.setBlobUrl(localPath.toString());
                logger.info("⬇️ Downloaded blob file to: {}", localPath);
            }

            writeAndUploadMetadataJson(message, batchDir);

            logger.info("📤 Calling OT orchestration API");
            OTResponse otResponse = callOrchestrationBatchApi("<REDACTED_TOKEN>", message);
            if (otResponse == null) {
                logger.error("❌ OT orchestration API failed");
                kafkaTemplate.send(kafkaOutputTopic, "{\"status\":\"FAILURE\",\"message\":\"OT call failed\"}");
                ack.acknowledge();
                return;
            }

            Map<String, Object> pendingMsg = Map.of(
                    "batchID", batchId,
                    "status", "PENDING",
                    "message", "OT Request Sent"
            );
            kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(pendingMsg));
            logger.info("🟡 OT request sent and acknowledged with PENDING status");

            ack.acknowledge();

            executor.submit(() -> processAfterOT(message, otResponse));

        } catch (Exception ex) {
            logger.error("❌ Kafka processing failed", ex);
        }
    }

    private void processAfterOT(KafkaMessage message, OTResponse otResponse) {
        logger.info("⏳ Starting post-OT processing for jobId={}, batchId={}...", otResponse.getJobId(), message.getBatchId());
        try {
            File xmlFile = waitForXmlFile(otResponse.getJobId(), otResponse.getId());
            if (xmlFile == null) throw new IllegalStateException("XML not found");

            logger.info("📑 Parsing XML file: {}", xmlFile.getAbsolutePath());
            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(xmlFile);
            doc.getDocumentElement().normalize();

            Map<String, String> accountCustomerMap = extractAccountCustomerMapFromDoc(doc);
            logger.info("🔍 Extracted {} customer entries from XML", accountCustomerMap.size());

            Path jobDir = Paths.get(mountPath, "output", message.getSourceSystem(), otResponse.getJobId());

            List<SummaryProcessedFile> processedFiles = buildAndUploadProcessedFiles(jobDir, accountCustomerMap, message);
            logger.info("📦 Uploaded {} processed files", processedFiles.size());

            String errorReportPath = Paths.get(jobDir.toString(), "ErrorReport.csv").toString();
            List<SummaryProcessedFile> failures = appendFailureEntries(errorReportPath, accountCustomerMap);
            processedFiles.addAll(failures);
            logger.info("⚠️ Appended {} failure entries", failures.size());

            List<PrintFile> printFiles = uploadPrintFiles(jobDir, message);
            logger.info("🖨️ Uploaded {} print files", printFiles.size());

            String triggerPath = jobDir.resolve("mobstat_trigger/DropData.trigger").toString();
            if (Files.exists(Paths.get(triggerPath))) {
                blobStorageService.uploadFile(new File(triggerPath), message.getSourceSystem() + "/mobstat_trigger/DropData.trigger");
                logger.info("🚀 Trigger file uploaded to blob: {}", triggerPath);
            }

            SummaryPayload payload = SummaryJsonWriter.buildPayload(message, processedFiles, printFiles, triggerPath, 0);
            String summaryPath = SummaryJsonWriter.writeSummaryJsonToFile(payload);
            String summaryUrl = blobStorageService.uploadSummaryJson(summaryPath, message, "summary_" + message.getBatchId() + ".json");
            payload.setSummaryFileURL(decodeUrl(summaryUrl));

            logger.info("📄 Summary JSON built and uploaded: {}", payload.getSummaryFileURL());
            logger.info("📄 Summary JSON content: \n{}", objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(payload));

            SummaryResponse response = new SummaryResponse();
            response.setBatchID(message.getBatchId());
            response.setFileName(payload.getFileName());
            response.setHeader(payload.getHeader());
            response.setMetadata(payload.getMetadata());
            response.setPayload(payload.getPayload());
            response.setSummaryFileURL(payload.getSummaryFileURL());

            ApiResponse finalResponse = new ApiResponse("Summary generated", "COMPLETED", response);
            logger.info("📤 Final response sent to Kafka:");
            logger.info(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(finalResponse));

            kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(finalResponse));
            logger.info("✅ Summary successfully published to output topic");

        } catch (Exception e) {
            logger.error("❌ Error post-OT summary generation", e);
        }
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
