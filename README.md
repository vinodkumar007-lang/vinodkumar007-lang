package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.*;
import com.nedbank.kafka.filemanage.utils.SummaryJsonWriter;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
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

    @KafkaListener(topics = "${kafka.topic.input}", groupId = "${kafka.consumer.group.id}")
    public void onKafkaMessage(String rawMessage, Acknowledgment ack) {
        try {
            logger.info("\uD83D\uDCE5 Received Kafka message: {}", rawMessage);
            KafkaMessage message = objectMapper.readValue(rawMessage, KafkaMessage.class);
            String batchId = message.getBatchId();
            Path batchDir = Paths.get(mountPath, "input", message.getSourceSystem(), batchId);
            Files.createDirectories(batchDir);
            logger.info("\uD83D\uDCC1 Created input directory: {}", batchDir);

            for (BatchFile file : message.getBatchFiles()) {
                String blobUrl = file.getBlobUrl();
                String content = blobStorageService.downloadFileContent(blobUrl);
                Path localPath = batchDir.resolve(file.getFilename());
                Files.write(localPath, content.getBytes(StandardCharsets.UTF_8));
                file.setBlobUrl(localPath.toString());
                logger.info("⬇️ Downloaded file {} to local path {}", blobUrl, localPath);
            }

            String url = switch (message.getSourceSystem().toUpperCase()) {
                case "DEBTMAN" -> otOrchestrationApiUrl;
                case "MFC" -> orchestrationMfcUrl;
                default -> throw new IllegalArgumentException("Unsupported source system: " + message.getSourceSystem());
            };

            logger.info("\uD83D\uDE80 Calling Orchestration API: {}", url);
            OTResponse otResponse = callOrchestrationBatchApi(orchestrationAuthToken, url, message);

            if (otResponse == null) {
                logger.error("❌ OT orchestration failed for batch {}", batchId);
                kafkaTemplate.send(kafkaOutputTopic, "{\"status\":\"FAILURE\",\"message\":\"OT call failed\"}");
                ack.acknowledge();
                return;
            }

            kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(Map.of(
                    "batchID", batchId,
                    "status", "PENDING",
                    "message", "OT Request Sent"
            )));
            logger.info("\uD83D\uDCE4 OT request sent for batch {}", batchId);
            ack.acknowledge();
            executor.submit(() -> processAfterOT(message, otResponse));

        } catch (Exception ex) {
            logger.error("❌ Kafka processing failed", ex);
        }
    }

    private List<CustomerSummary> parseSTDXml(File xmlFile, Map<String, Map<String, String>> errorMap) {
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
                    String val = queues.item(q).getTextContent().trim().toUpperCase();
                    if (!val.isEmpty()) methods.add(val);
                }

                if (acc != null && cis != null) {
                    CustomerSummary cs = new CustomerSummary();
                    cs.setAccountNumber(acc);
                    cs.setCisNumber(cis);
                    cs.setCustomerId(acc);

                    // Merge error report
                    Map<String, String> deliveryStatus = errorMap.getOrDefault(acc, new HashMap<>());
                    cs.setDeliveryStatus(deliveryStatus); // for logs only

                    long failed = methods.stream()
                            .filter(m -> "FAILED".equalsIgnoreCase(deliveryStatus.getOrDefault(m, "")))
                            .count();

                    if (failed == methods.size()) {
                        cs.setStatus("FAILED");
                    } else if (failed > 0) {
                        cs.setStatus("PARTIAL");
                    } else {
                        cs.setStatus("SUCCESS");
                    }

                    list.add(cs);

                    logger.debug("📋 Customer: {}, CIS: {}, Methods: {}, Failed: {}, FinalStatus: {}",
                            acc, cis, methods, failed, cs.getStatus());
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

            // ✅ Parse error report
            Map<String, Map<String, String>> errorMap = parseErrorReport(message);
            logger.info("🧾 Parsed error report with {} entries", errorMap.size());

            // ✅ Parse STD XML for customer summaries
            List<CustomerSummary> customerSummaries = parseSTDXml(xmlFile, errorMap);
            logger.info("📊 Total customerSummaries parsed: {}", customerSummaries.size());

            // ✅ Build jobDir path
            Path jobDir = Paths.get(mountPath, "output", message.getSourceSystem(), otResponse.getJobId());

            // ✅ Build processedFiles using fixed method call
            List<SummaryProcessedFile> processedFiles =
                    buildDetailedProcessedFiles(jobDir, errorMap, message);
            logger.info("📦 Processed {} customer records", processedFiles.size());

            // ✅ Upload print files
            List<PrintFile> printFiles = uploadPrintFiles(jobDir, message);
            logger.info("🖨️ Uploaded {} print files", printFiles.size());

            // ✅ Upload mobstat trigger if present
            String mobstatTriggerUrl = findAndUploadMobstatTriggerFile(jobDir, message);
            String currentTimestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now());

            // ✅ Build final payload
            SummaryPayload payload = SummaryJsonWriter.buildPayload(
                    message, processedFiles, printFiles, mobstatTriggerUrl, processedFiles.size());

            payload.setFileName(message.getBatchFiles().get(0).getFilename());
            payload.setTimestamp(currentTimestamp);
            if (payload.getHeader() != null) {
                payload.getHeader().setTimestamp(currentTimestamp);
            }

            // ✅ Upload summary.json
            String fileName = "summary_" + message.getBatchId() + ".json";
            String summaryPath = SummaryJsonWriter.writeSummaryJsonToFile(payload);
            String summaryUrl = blobStorageService.uploadSummaryJson(summaryPath, message, fileName);
            payload.setSummaryFileURL(decodeUrl(summaryUrl));

            logger.info("📁 Summary JSON uploaded to: {}", decodeUrl(summaryUrl));
            logger.info("📄 Final Summary Payload:\n{}",
                    objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(payload));

            // ✅ Send response to Kafka
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

            logger.info("✅ Kafka output sent for batch {} with response: {}", message.getBatchId(),
                    objectMapper.writeValueAsString(response));

        } catch (Exception e) {
            logger.error("❌ Error post-OT summary generation", e);
        }
    }

    private String findAndUploadMobstatTriggerFile(Path jobDir, KafkaMessage message) {
        try (Stream<Path> stream = Files.list(jobDir)) {
            Optional<Path> trigger = stream.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().toLowerCase().endsWith(".trigger"))
                    .findFirst();
            if (trigger.isPresent()) {
                String blobUrl = blobStorageService.uploadFile(trigger.get().toFile(),
                        message.getSourceSystem() + "/" + message.getBatchId() + "/" + trigger.get().getFileName());
                return decodeUrl(blobUrl);
            } else {
                logger.info("ℹ️ No .trigger file found in jobDir: {}", jobDir);
            }
        } catch (IOException e) {
            logger.warn("⚠️ Failed to scan for .trigger file", e);
        }
        return null;
    }

    private OTResponse callOrchestrationBatchApi(String token, String url, KafkaMessage msg) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.set("Authorization", "Bearer " + token);
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<String> request = new HttpEntity<>(objectMapper.writeValueAsString(msg), headers);
            ResponseEntity<Map> response = restTemplate.exchange(url, HttpMethod.POST, request, Map.class);

            List<Map<String, Object>> data = (List<Map<String, Object>>) response.getBody().get("data");
            if (data != null && !data.isEmpty()) {
                Map<String, Object> item = data.get(0);
                OTResponse otResponse = new OTResponse();
                otResponse.setJobId((String) item.get("jobId"));
                otResponse.setId((String) item.get("id"));
                return otResponse;
            } else {
                logger.error("❌ No data in OT orchestration response");
            }
        } catch (Exception e) {
            logger.error("❌ Failed OT Orchestration call", e);
        }
        return null;
    }

    private File waitForXmlFile(String jobId, String id) throws InterruptedException {
        Path docgenRoot = Paths.get(mountPath, "jobs", jobId, id, "docgen");
        long startTime = System.currentTimeMillis();
        File xmlFile = null;

        while ((System.currentTimeMillis() - startTime) < rptMaxWaitSeconds * 1000L) {
            if (Files.exists(docgenRoot)) {
                try (Stream<Path> paths = Files.walk(docgenRoot)) {
                    Optional<Path> xmlPath = paths
                            .filter(Files::isRegularFile)
                            .filter(p -> p.getFileName().toString().equalsIgnoreCase("_STDDELIVERYFILE.xml"))
                            .findFirst();

                    if (xmlPath.isPresent()) {
                        xmlFile = xmlPath.get().toFile();

                        // ✅ Check file size is stable (not growing)
                        long size1 = xmlFile.length();
                        TimeUnit.SECONDS.sleep(1); // wait a second
                        long size2 = xmlFile.length();

                        if (size1 > 0 && size1 == size2) {
                            logger.info("✅ Found stable XML file: {}", xmlFile.getAbsolutePath());
                            return xmlFile;
                        } else {
                            logger.info("⌛ XML file still being written (size changing): {}", xmlFile.getAbsolutePath());
                        }
                    }
                } catch (IOException e) {
                    logger.warn("⚠️ Error scanning docgen folder", e);
                }
            } else {
                logger.debug("🔍 docgen folder not found yet: {}", docgenRoot);
            }

            TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
        }

        logger.error("❌ Timed out waiting for complete XML file in {}", docgenRoot);
        return null;
    }

    private Map<String, String> extractAccountCustomerMapFromDoc(Document doc) {
        Map<String, String> map = new HashMap<>();
        NodeList customers = doc.getElementsByTagName("customer");
        for (int i = 0; i < customers.getLength(); i++) {
            Element customer = (Element) customers.item(i);
            NodeList keys = customer.getElementsByTagName("key");
            String acc = null, cus = null;
            for (int j = 0; j < keys.getLength(); j++) {
                Element k = (Element) keys.item(j);
                if ("AccountNumber".equalsIgnoreCase(k.getAttribute("name"))) acc = k.getTextContent();
                if ("CISNumber".equalsIgnoreCase(k.getAttribute("name"))) cus = k.getTextContent();
            }
            if (acc != null && cus != null) map.put(acc, cus);
        }
        return map;
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
                    String method = parts[3].trim().toUpperCase();
                    String status = parts[4].trim();
                    map.computeIfAbsent(acc, k -> new HashMap<>()).put(method, status);
                } else if (parts.length >= 3) {
                    String acc = parts[0].trim();
                    String method = parts[2].trim().toUpperCase();
                    String status = parts.length > 3 ? parts[3].trim() : "Failed";
                    map.computeIfAbsent(acc, k -> new HashMap<>()).put(method, status);
                }
            }
        } catch (Exception e) {
            logger.warn("⚠️ Error reading ErrorReport.csv", e);
        }
        return map;
    }

    private List<SummaryProcessedFile> buildDetailedProcessedFiles(
            Path jobDir,
            Map<String, String> accountCustomerMap,
            KafkaMessage msg,
            Map<String, String> errorMap
    ) {
        Map<String, SummaryProcessedFile> customerFileMap = new HashMap<>();
        List<SummaryProcessedFile> finalList = new ArrayList<>();

        List<String> folders = List.of("archive", "email", "mobstat", "print");

        for (String folder : folders) {
            Path folderPath = jobDir.resolve(folder);
            if (!Files.exists(folderPath)) continue;

            try (Stream<Path> fileStream = Files.list(folderPath)) {
                fileStream.filter(Files::isRegularFile).forEach(file -> {
                    String fileName = file.getFileName().toString();
                    if (!fileName.contains("_")) return;

                    String accountNumber = fileName.split("_")[0];
                    String customerId = accountCustomerMap.getOrDefault(accountNumber, "UNKNOWN");
                    String key = accountNumber + "_" + customerId;

                    SummaryProcessedFile spf = customerFileMap.getOrDefault(key, new SummaryProcessedFile());
                    spf.setAccountNumber(accountNumber);
                    spf.setCustomerId(customerId);
                    spf.getFileUrls().put(folder, msg.getBlobUrl() + "/" + folder + "/" +fileName);
                    spf.setStatus("SUCCESS");

                    customerFileMap.put(key, spf);
                });
            } catch (IOException e) {
                logger.error("❌ Error reading folder {}: {}", folder, e.getMessage());
            }
        }

        // 🛑 Add errorMap-based missing file entries
        for (Map.Entry<String, String> entry : errorMap.entrySet()) {
            String key = entry.getKey(); // accountNumber_deliveryType
            String status = entry.getValue();

            String[] parts = key.split("_");
            if (parts.length != 2) continue;

            String accountNumber = parts[0];
            String deliveryType = parts[1];
            String customerId = accountCustomerMap.getOrDefault(accountNumber, "UNKNOWN");
            String combinedKey = accountNumber + "_" + customerId;

            SummaryProcessedFile spf = customerFileMap.getOrDefault(combinedKey, new SummaryProcessedFile());
            spf.setAccountNumber(accountNumber);
            spf.setCustomerId(customerId);

            if (!spf.getFileUrls().containsKey(deliveryType)) {
                spf.getFileUrls().put(deliveryType, "");
                spf.setStatus("FAILED");
            }

            customerFileMap.put(combinedKey, spf);
        }

        finalList.addAll(customerFileMap.values());

        // ➕ Add mobstat_trigger files separately
        Path mobstatTriggerPath = jobDir.resolve("mobstat_trigger");
        if (Files.exists(mobstatTriggerPath)) {
            try (Stream<Path> triggerFiles = Files.list(mobstatTriggerPath)) {
                triggerFiles.filter(Files::isRegularFile).forEach(file -> {
                    SummaryProcessedFile trigger = new SummaryProcessedFile();
                    trigger.setFileType("mobstat_trigger");
                    trigger.setFileURL(msg.getBlobUrl() + "/mobstat_trigger/" + file.getFileName());
                    finalList.add(trigger); // Not added to customerFileMap, won't affect count
                });
            } catch (IOException e) {
                logger.error("❌ Error reading mobstat_trigger folder: {}", e.getMessage());
            }
        }

        logger.info("✅ Total unique customer delivery records (excluding trigger): {}", customerFileMap.size());
        return finalList;
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
