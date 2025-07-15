// ✅ Fully Updated KafkaListenerService with DEBTMAN/MFC logic and all enhancements
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
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.*;
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

    @Value("${ot.service.debtman}")
    private String otServiceDebtman;

    @Value("${ot.service.mfc}")
    private String otServiceMfc;

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
        logger.info("📩 Received Kafka message");
        try {
            KafkaMessage message = objectMapper.readValue(rawMessage, KafkaMessage.class);
            logger.info("🔍 Parsed Kafka message with batchId: {}", message.getBatchId());

            List<BatchFile> dataFiles = message.getBatchFiles().stream()
                    .filter(f -> "DATA".equalsIgnoreCase(f.getFileType()))
                    .toList();
            message.setBatchFiles(dataFiles);

            Path batchDir = Paths.get(mountPath, "input", message.getSourceSystem(), message.getBatchId());
            Files.createDirectories(batchDir);

            for (BatchFile file : dataFiles) {
                String content = blobStorageService.downloadFileContent(file.getBlobUrl());
                Path localPath = batchDir.resolve(message.getSourceSystem() + ".csv");
                Files.write(localPath, content.getBytes(StandardCharsets.UTF_8));
                file.setBlobUrl(localPath.toString());
            }

            writeAndUploadMetadataJson(message, batchDir);

            String otServiceUrl = resolveOTService(message.getSourceSystem());
            OTResponse otResponse = callOrchestrationBatchApi(message, otServiceUrl);
            if (otResponse == null) {
                kafkaTemplate.send(kafkaOutputTopic, "{\"status\":\"FAILURE\",\"message\":\"OT call failed\"}");
                ack.acknowledge();
                return;
            }

            Map<String, Object> pendingMsg = Map.of(
                    "batchID", message.getBatchId(),
                    "status", "PENDING",
                    "message", "OT Request Sent"
            );
            kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(pendingMsg));
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

            Map<String, SummaryProcessedFile> customerMap = parseOutputXmlToMap(xmlFile);
            int totalProcessed = customerMap.size();

            Path jobDir = Paths.get(mountPath, "output", message.getSourceSystem(), otResponse.getJobId());
            Path errorReport = jobDir.resolve("ErrorReport.csv");
            Map<String, Map<String, String>> errorMap = readErrorReportToMap(errorReport);

            List<SummaryProcessedFile> processedFiles = buildDetailedProcessedFiles(jobDir, customerMap, errorMap, message);
            List<PrintFile> printFiles = uploadPrintFiles(jobDir, message);

            String triggerPath = jobDir.resolve("mobstat_trigger/DropData.trigger").toString();
            if (Files.exists(Paths.get(triggerPath))) {
                blobStorageService.uploadFile(new File(triggerPath), message.getSourceSystem() + "/mobstat_trigger/DropData.trigger");
            }

            SummaryPayload payload = SummaryJsonWriter.buildPayload(message, processedFiles, printFiles, triggerPath, totalProcessed);
            payload.setFileName(message.getSourceSystem() + ".csv");
            payload.setTimestamp(Instant.now().toString());

            String summaryPath = SummaryJsonWriter.writeSummaryJsonToFile(payload);
            String summaryUrl = blobStorageService.uploadSummaryJson(summaryPath, message, "summary_" + message.getBatchId() + ".json");
            payload.setSummaryFileURL(decodeUrl(summaryUrl));

            SummaryResponse response = new SummaryResponse();
            response.setBatchID(message.getBatchId());
            response.setFileName(payload.getFileName());
            response.setHeader(payload.getHeader());
            response.setMetadata(payload.getMetadata());
            response.setPayload(payload.getPayload());
            response.setSummaryFileURL(payload.getSummaryFileURL());
            response.setTimestamp(payload.getTimestamp());

            ApiResponse finalResponse = new ApiResponse("Summary generated", "COMPLETED", response);
            kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(finalResponse));

        } catch (Exception e) {
            logger.error("❌ Error post-OT summary generation", e);
        }
    }

    private String resolveOTService(String sourceSystem) {
        return switch (sourceSystem.toUpperCase()) {
            case "DEBTMAN" -> otServiceDebtman;
            case "MFC" -> otServiceMfc;
            default -> otServiceDebtman;
        };
    }

    private OTResponse callOrchestrationBatchApi(KafkaMessage msg, String otServiceUrl) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<String> request = new HttpEntity<>(objectMapper.writeValueAsString(msg), headers);
            ResponseEntity<Map> response = restTemplate.exchange(otServiceUrl, HttpMethod.POST, request, Map.class);
            List<Map<String, Object>> data = (List<Map<String, Object>>) response.getBody().get("data");
            if (data != null && !data.isEmpty()) {
                Map<String, Object> item = data.get(0);
                OTResponse otResponse = new OTResponse();
                otResponse.setJobId((String) item.get("jobId"));
                otResponse.setId((String) item.get("id"));
                return otResponse;
            }
        } catch (Exception e) {
            logger.error("❌ Failed OT Orchestration call", e);
        }
        return null;
    }

    private void writeAndUploadMetadataJson(KafkaMessage message, Path jobDir) {
        try {
            Map<String, Object> metaMap = objectMapper.convertValue(message, Map.class);
            File metaFile = new File(jobDir.toFile(), "metadata.json");
            FileUtils.writeStringToFile(metaFile, objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(metaMap), StandardCharsets.UTF_8);
            blobStorageService.uploadFile(metaFile.getAbsolutePath(), message.getSourceSystem() + "/Trigger/metadata.json");
        } catch (Exception e) {
            logger.warn("Failed to write metadata.json", e);
        }
    }

    private File waitForXmlFile(String jobId, String id) throws InterruptedException {
        Path docgenRoot = Paths.get(mountPath, "jobs", jobId, id, "docgen");
        long startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < rptMaxWaitSeconds * 1000L) {
            if (!Files.exists(docgenRoot)) {
                TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
                continue;
            }
            try (Stream<Path> paths = Files.walk(docgenRoot)) {
                Optional<Path> xmlPath = paths.filter(Files::isRegularFile).filter(p -> p.getFileName().toString().equalsIgnoreCase("_STDDELIVERYFILE.xml")).findFirst();
                if (xmlPath.isPresent()) return xmlPath.get().toFile();
            } catch (IOException e) {
                logger.warn("⚠️ Error scanning docgen folder", e);
            }
            TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
        }
        return null;
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
