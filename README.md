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
import java.time.format.DateTimeFormatter;
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

    // ✅ NEW: Parse delivery output statuses from STD XML
    private List<CustomerSummary> parseSTDXml(File xmlFile, List<String> errorFiles) {
        List<CustomerSummary> list = new ArrayList<>();
        try {
            Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(xmlFile);
            doc.getDocumentElement().normalize();

            NodeList customers = doc.getElementsByTagName("customer");
            for (int i = 0; i < customers.getLength(); i++) {
                Element cust = (Element) customers.item(i);

                String acc = null, cis = null;
                List<String> methods = new ArrayList<>();

                NodeList keys = cust.getElementsByTagName("key");
                for (int j = 0; j < keys.getLength(); j++) {
                    Element k = (Element) keys.item(j);
                    if ("AccountNumber".equalsIgnoreCase(k.getAttribute("name"))) acc = k.getTextContent();
                    if ("CISNumber".equalsIgnoreCase(k.getAttribute("name"))) cis = k.getTextContent();
                }

                NodeList queues = cust.getElementsByTagName("queueName");
                for (int q = 0; q < queues.getLength(); q++) {
                    String val = queues.item(q).getTextContent().trim().toLowerCase();
                    if (!val.isEmpty()) methods.add(val);
                }

                if (acc != null && cis != null) {
                    CustomerSummary cs = new CustomerSummary();
                    cs.setAccountNumber(acc);
                    cs.setCisNumber(cis);
                    cs.setOutputMethods(methods);

                    if (methods.stream().anyMatch(errorFiles::contains)) {
                        cs.setStatus(methods.size() > 1 ? "PARTIAL" : "FAILED");
                    } else {
                        cs.setStatus("SUCCESS");
                    }
                    list.add(cs);
                }
            }
        } catch (Exception e) {
            logger.error("❌ Failed parsing STD XML", e);
        }
        return list;
    }

    private void processAfterOT(KafkaMessage message, OTResponse otResponse) {
        try {
            logger.info("⏳ Waiting for XML for jobId={}, id={}", otResponse.getJobId(), otResponse.getId());
            File xmlFile = waitForXmlFile(otResponse.getJobId(), otResponse.getId());
            if (xmlFile == null) throw new IllegalStateException("XML not found");
            logger.info("✅ Found XML file: {}", xmlFile);

            List<String> errorFiles = List.of("print", "html", "mobstat", "email", "txt", "archive");
            List<CustomerSummary> customerSummaries = parseSTDXml(xmlFile, errorFiles);

            Map<String, String> accountCustomerMap = new HashMap<>();
            for (CustomerSummary cs : customerSummaries) {
                accountCustomerMap.put(cs.getAccountNumber(), cs.getCisNumber());
            }

            Map<String, Map<String, String>> errorMap = parseErrorReport(message);
            logger.info("🧾 Parsed error report with {} entries", errorMap.size());

            Path jobDir = Paths.get(mountPath, "output", message.getSourceSystem(), otResponse.getJobId());
            List<SummaryProcessedFile> processedFiles = buildDetailedProcessedFiles(jobDir, new HashMap<>(), errorMap, message);
            logger.info("📦 Processed {} customer records", processedFiles.size());

            List<PrintFile> printFiles = uploadPrintFiles(jobDir, message);
            logger.info("🖨️ Uploaded {} print files", printFiles.size());

            String mobstatTriggerUrl = findAndUploadMobstatTriggerFile(jobDir, message);
            String currentTimestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now());

            SummaryPayload payload = SummaryJsonWriter.buildPayload(
                    message, processedFiles, printFiles, mobstatTriggerUrl, accountCustomerMap.size());

            payload.setFileName(message.getBatchFiles().get(0).getFilename());
            payload.setTimestamp(currentTimestamp);

            if (payload.getHeader() != null) {
                payload.getHeader().setTimestamp(currentTimestamp);
            }

            String fileName = "summary_" + message.getBatchId() + ".json";
            String summaryPath = SummaryJsonWriter.writeSummaryJsonToFile(payload);
            String summaryUrl = blobStorageService.uploadSummaryJson(summaryPath, message, fileName);

            payload.setSummaryFileURL(decodeUrl(summaryUrl));

            logger.info("📁 Summary JSON uploaded to: {}", decodeUrl(summaryUrl));
            logger.info("📄 Final Summary Payload:\n{}",
                    objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(payload));

            SummaryResponse response = new SummaryResponse();
            response.setBatchID(message.getBatchId());
            response.setFileName(payload.getFileName());
            response.setHeader(payload.getHeader());
            response.setMetadata(payload.getMetadata());
            response.setPayload(payload.getPayload());
            response.setSummaryFileURL(decodeUrl(summaryUrl));
            response.setTimestamp(currentTimestamp);

            kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(
                    new ApiResponse("Summary generated", "COMPLETED", response)));

            logger.info("✅ Kafka output sent for batch {} with response: {}", message.getBatchId(), objectMapper.writeValueAsString(response));

        } catch (Exception e) {
            logger.error("❌ Error post-OT summary generation", e);
        }
    }

    @PreDestroy
    public void shutdownExecutor() {
        logger.info("⚠️ Shutting down executor service");
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
