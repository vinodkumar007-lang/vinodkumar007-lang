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
import java.util.concurrent.atomic.AtomicReference;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
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

    public List<SummaryProcessedFile> parseSTDXml(File stdXmlFile) {
        List<SummaryProcessedFile> entries = new ArrayList<>();
        try {
            BufferedReader reader = new BufferedReader(new FileReader(stdXmlFile));
            String line;
            while ((line = reader.readLine()) != null) {
                if (line.contains("<file name=")) {
                    String name = extractAttribute(line, "name");
                    String queueName = extractAttribute(line, "name", "queue");
                    if (name != null && queueName != null) {
                        String[] parts = name.split("/");
                        String fileName = parts[parts.length - 1];

                        String[] nameParts = fileName.split("_");
                        String cis = nameParts.length > 0 ? nameParts[0] : "";
                        String account = nameParts.length > 1 ? nameParts[1] : "";

                        SummaryProcessedFile entry = new SummaryProcessedFile();
                        entry.setQueueName(queueName);
                        entry.setCustomerId(cis);
                        entry.setAccountNumber(account);
                        entry.setStatus("success");
                        entry.setFileUrl(name);
                        entries.add(entry);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("Error parsing STD XML file", e);
        }
        return entries;
    }

    private String extractAttribute(String line, String attribute) {
        Pattern pattern = Pattern.compile(attribute + "=\"(.*?)\"");
        Matcher matcher = pattern.matcher(line);
        return matcher.find() ? matcher.group(1) : null;
    }

    private String extractAttribute(String line, String attribute, String tagName) {
        if (!line.contains("<" + tagName)) return null;
        return extractAttribute(line, attribute);
    }

    private void processAfterOT(KafkaMessage message, OTResponse otResponse) {
        try {
            logger.info("⏳ Waiting for XML for jobId={}, id={}", otResponse.getJobId(), otResponse.getId());
            File xmlFile = waitForXmlFile(otResponse.getJobId(), otResponse.getId());
            if (xmlFile == null) throw new IllegalStateException("XML not found");
            logger.info("✅ Found XML file: {}", xmlFile);

            // ✅ Parse error report (already used in buildDetailedProcessedFiles)
            Map<String, Map<String, String>> errorMap = parseErrorReport(String.valueOf(xmlFile));
            logger.info("🧾 Parsed error report with {} entries", errorMap.size());

// ✅ Parse STDXML and extract basic customer summaries
            List<SummaryProcessedFile> customerSummaries = parseSTDXml(xmlFile);
            logger.info("📊 Total customerSummaries parsed: {}", customerSummaries.size());
            // ✅ Convert to basic SummaryProcessedFile list
            List<SummaryProcessedFile> customerList = customerSummaries.stream()
                    .map(cs -> {
                        SummaryProcessedFile spf = new SummaryProcessedFile();
                        spf.setAccountNumber(cs.getAccountNumber());
                        spf.setCustomerId(cs.getCustomerId());
                        return spf;
                    })
                    .collect(Collectors.toList());

            // ✅ Locate output job directory
            Path jobDir = Paths.get(mountPath, "output", message.getSourceSystem(), otResponse.getJobId());

            // ✅ Build processedFiles with output-specific blob URLs and status (SUCCESS/ERROR)
            List<SummaryProcessedFile> processedFiles =
                    buildDetailedProcessedFiles(jobDir, customerList, errorMap, message);
            logger.info("📦 Processed {} customer records", processedFiles.size());

            // ✅ Upload print files and track their URLs
            List<PrintFile> printFiles = uploadPrintFiles(jobDir, message);
            logger.info("🖨️ Uploaded {} print files", printFiles.size());

            // ✅ Upload MobStat trigger file if present
            String mobstatTriggerUrl = findAndUploadMobstatTriggerFile(jobDir, message);

            // ✅ Extract counts from <outputList> inside STD XML
            Map<String, Integer> summaryCounts = extractSummaryCountsFromXml(xmlFile);
            String customersProcessed = String.valueOf(summaryCounts.getOrDefault("customersProcessed", processedFiles.size()));
            String pagesProcessed = String.valueOf(summaryCounts.getOrDefault("pagesProcessed", 0));

            // ✅ Create payload for summary.json
            SummaryPayload payload = SummaryJsonWriter.buildPayload(
                    message,
                    processedFiles,      // ✅ Now includes blob URLs + status from buildDetailedProcessedFiles
                    pagesProcessed,
                    printFiles.toString(),
                    mobstatTriggerUrl,
                    customersProcessed
            );

            String currentTimestamp = DateTimeFormatter.ISO_INSTANT.format(Instant.now());
            payload.setFileName(message.getBatchFiles().get(0).getFilename());
            payload.setTimestamp(currentTimestamp);
            if (payload.getHeader() != null) {
                payload.getHeader().setTimestamp(currentTimestamp);
            }

            // ✅ Write and upload summary.json
            String fileName = "summary_" + message.getBatchId() + ".json";
            String summaryPath = SummaryJsonWriter.writeSummaryJsonToFile(payload);
            String summaryUrl = blobStorageService.uploadSummaryJson(summaryPath, message, fileName);
            payload.setSummaryFileURL(decodeUrl(summaryUrl));
            logger.info("📁 Summary JSON uploaded to: {}", decodeUrl(summaryUrl));

            // ✅ Final beautified payload log
            logger.info("📄 Final Summary Payload:\n{}",
                    objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(payload));

            // ✅ Send final response to Kafka
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

    public List<ErrorReportEntry> parseErrorReport(Path errorReportPath) {
        List<ErrorReportEntry> errorEntries = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(errorReportPath.toFile()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                String[] parts = line.split("\\|");
                if (parts.length >= 5) {
                    ErrorReportEntry entry = new ErrorReportEntry();
                    entry.setCustomerId(parts[0].trim());
                    entry.setAccountNumber(parts[1].trim());
                    entry.setOutputMethod(parts[3].trim().toLowerCase());
                    entry.setErrorType(parts[2].trim());
                    entry.setErrorStatus(parts[4].trim());
                    errorEntries.add(entry);
                }
            }
        } catch (Exception e) {
            logger.error("Error parsing error report file", e);
        }
        return errorEntries;
    }

    private List<SummaryProcessedFile> buildDetailedProcessedFiles(
            Path jobDir,
            List<SummaryProcessedFile> customerList,
            Map<String, Map<String, String>> errorMap,
            KafkaMessage message
    ) {
        List<SummaryProcessedFile> processedFiles = new ArrayList<>();
        Map<String, SummaryProcessedFile> outputMap = new HashMap<>();

        for (SummaryProcessedFile customer : customerList) {
            String customerId = customer.getCustomerId();
            String accountNumber = customer.getAccountNumber();

            for (String type : Arrays.asList("email", "print", "mobstat", "archive")) {
                try {
                    // Build expected file name or directory path
                    Path targetFolder = jobDir.resolve(type);
                    Optional<Path> match = findCustomerFile(targetFolder, customerId, accountNumber);

                    SummaryProcessedFile entry = new SummaryProcessedFile();
                    entry.setCustomerId(customerId);
                    entry.setAccountNumber(accountNumber);
                    entry.setOutputType(type.toUpperCase());

                    if (match.isPresent()) {
                        Path file = match.get();

                        // Upload file to blob
                        String blobUrl = blobStorageService.uploadFileByMessage(
                                file.toFile(),
                                message.getSourceSystem(),
                               message
                        );
                        entry.setBlobURL(blobUrl);
                        entry.setStatus("SUCCESS");
                    } else {
                        entry.setStatus("ERROR");
                        entry.setBlobURL(null);
                    }

                    // Check if errors exist for this customer+account
                    String key = customerId + "::" + accountNumber;
                    if (errorMap.containsKey(key)) {
                        Map<String, String> details = errorMap.get(key);
                        List<ErrorDetail> errorDetails = details.entrySet().stream()
                                .map(e -> new ErrorDetail(e.getKey(), e.getValue()))
                                .collect(Collectors.toList());

                        ErrorReportEntry errorEntry = new ErrorReportEntry();
                        errorEntry.setCustomerId(customerId);
                        errorEntry.setAccountNumber(accountNumber);
                        errorEntry.setErrorDetails(errorDetails);

                        entry.setErrorReportEntry(String.valueOf(errorEntry));
                    }

                    processedFiles.add(entry);

                } catch (Exception e) {
                    logger.error("❌ Failed processing customer={} account={} type={}", customerId, accountNumber, type, e);
                }
            }
        }

        return processedFiles;
    }

    private Optional<Path> findCustomerFile(Path folder, String customerId, String accountNumber) {
        try (Stream<Path> files = Files.walk(folder)) {
            return files
                    .filter(Files::isRegularFile)
                    .filter(path -> {
                        String fileName = path.getFileName().toString().toLowerCase();
                        return fileName.contains(customerId.toLowerCase()) && fileName.contains(accountNumber.toLowerCase());
                    })
                    .findFirst();
        } catch (IOException e) {
            logger.warn("⚠️ Could not search files in folder: {}", folder);
            return Optional.empty();
        }
    }

    // Helper methods
    private boolean isNumeric(String str) {
        return str != null && str.matches("\\d+");
    }

    private SummaryProcessedFile buildCopy(SummaryProcessedFile original) {
        SummaryProcessedFile copy = new SummaryProcessedFile();
        BeanUtils.copyProperties(original, copy);
        return copy;
    }

    private Map<String, Map<String, String>> parseErrorReport(String content) {
        Map<String, Map<String, String>> errorMap = new HashMap<>();

        for (String line : content.split("\\r?\\n")) {
            String[] parts = line.split("\\|");
            if (parts.length >= 5) {
                String customer = parts[0].trim();
                String account = parts[1].trim();
                String method = parts[3].trim().toUpperCase(); // EMAIL, PRINT, etc.

                Map<String, String> info = new HashMap<>();
                info.put("account", account);
                info.put("method", method);

                errorMap.put(customer, info);
            }
        }
        return errorMap;
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

    private Map<String, Integer> extractSummaryCountsFromXml(File xmlFile) {
        Map<String, Integer> summaryCounts = new HashMap<>();
        try {
            Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(xmlFile);
            doc.getDocumentElement().normalize();
            NodeList outputListNodes = doc.getElementsByTagName("outputList");
            if (outputListNodes.getLength() > 0) {
                Element outputList = (Element) outputListNodes.item(0);
                String customersProcessed = outputList.getAttribute("customersProcessed");
                String pagesProcessed = outputList.getAttribute("pagesProcessed");

                int custCount = customersProcessed != null && !customersProcessed.isBlank()
                        ? Integer.parseInt(customersProcessed) : 0;
                int pageCount = pagesProcessed != null && !pagesProcessed.isBlank()
                        ? Integer.parseInt(pagesProcessed) : 0;

                summaryCounts.put("customersProcessed", custCount);
                summaryCounts.put("pagesProcessed", pageCount);
            }
        } catch (Exception e) {
            logger.warn("⚠️ Unable to extract summary counts from XML", e);
        }
        return summaryCounts;
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


===========================

package com.nedbank.kafka.filemanage.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.nedbank.kafka.filemanage.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

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

    public static SummaryPayload buildPayload(
            KafkaMessage kafkaMessage,
            List<SummaryProcessedFile> processedList,
            String summaryBlobUrl,
            String fileName,
            String batchId,
            String timestamp
    ) {
        SummaryPayload payload = new SummaryPayload();
        payload.setBatchID(batchId);
        payload.setFileName(fileName);
        payload.setTimestamp(timestamp);
        payload.setSummaryFileURL(summaryBlobUrl);

        Header header = new Header();
        header.setTenantCode(kafkaMessage.getTenantCode());
        header.setChannelID(kafkaMessage.getChannelID());
        header.setAudienceID(kafkaMessage.getAudienceID());
        header.setTimestamp(timestamp);
        header.setSourceSystem(kafkaMessage.getSourceSystem());
        header.setProduct(kafkaMessage.getSourceSystem());
        header.setJobName(kafkaMessage.getSourceSystem());
        payload.setHeader(header);

        List<ProcessedFileEntry> processedFileEntries = buildProcessedFileEntries(processedList);
        payload.setProcessedFileList(processedFileEntries);

        int totalFileUrls = processedFileEntries.stream()
                .mapToInt(entry -> {
                    int count = 0;
                    if (entry.getPdfEmailFileUrl() != null && !entry.getPdfEmailFileUrl().isBlank()) count++;
                    if (entry.getPdfArchiveFileUrl() != null && !entry.getPdfArchiveFileUrl().isBlank()) count++;
                    if (entry.getPdfMobstatFileUrl() != null && !entry.getPdfMobstatFileUrl().isBlank()) count++;
                    if (entry.getPrintFileUrl() != null && !entry.getPrintFileUrl().isBlank()) count++;
                    return count;
                })
                .sum();

        Payload payloadInfo = new Payload();
        payloadInfo.setUniqueECPBatchRef(kafkaMessage.getUniqueECPBatchRef());
        payloadInfo.setRunPriority(kafkaMessage.getRunPriority());
        payloadInfo.setEventID(kafkaMessage.getEventID());
        payloadInfo.setEventType(kafkaMessage.getEventType());
        payloadInfo.setRestartKey(kafkaMessage.getRestartKey());
        payloadInfo.setFileCount(totalFileUrls);
        payload.setPayload(payloadInfo);

        Metadata metadata = new Metadata();
        metadata.setTotalCustomersProcessed((int) processedFileEntries.stream()
                .map(pf -> pf.getCustomerId() + "::" + pf.getAccountNumber())
                .distinct()
                .count());

        long total = processedFileEntries.size();
        long success = processedFileEntries.stream()
                .filter(entry -> "SUCCESS".equalsIgnoreCase(entry.getOverAllStatusCode()))
                .count();
        long failed = processedFileEntries.stream()
                .filter(entry -> "FAILED".equalsIgnoreCase(entry.getOverAllStatusCode()))
                .count();

        String overallStatus;
        if (success == total) {
            overallStatus = "SUCCESS";
        } else if (failed == total) {
            overallStatus = "FAILED";
        } else {
            overallStatus = "PARTIAL";
        }

        metadata.setProcessingStatus(overallStatus);
        metadata.setEventOutcomeCode("0");
        metadata.setEventOutcomeDescription(overallStatus.toLowerCase());
        payload.setMetadata(metadata);

        return payload;
    }

    private static List<ProcessedFileEntry> buildProcessedFileEntries(List<SummaryProcessedFile> processedList) {
        List<ProcessedFileEntry> finalList = new ArrayList<>();

        Map<String, List<SummaryProcessedFile>> grouped = processedList.stream()
                .filter(f -> f.getCustomerId() != null && f.getAccountNumber() != null)
                .collect(Collectors.groupingBy(f -> f.getCustomerId() + "::" + f.getAccountNumber()));

        for (Map.Entry<String, List<SummaryProcessedFile>> group : grouped.entrySet()) {
            String[] parts = group.getKey().split("::");
            String customerId = parts[0];
            String accountNumber = parts[1];

            Map<String, SummaryProcessedFile> methodMap = new HashMap<>();
            Map<String, SummaryProcessedFile> archiveMap = new HashMap<>();

            for (SummaryProcessedFile file : group.getValue()) {
                String method = file.getOutputMethod();
                if (method == null) continue;

                switch (method.toUpperCase()) {
                    case "EMAIL", "MOBSTAT", "PRINT" -> methodMap.put(method.toUpperCase(), file);
                    case "ARCHIVE" -> {
                        String linked = file.getLinkedDeliveryType();
                        if (linked != null) {
                            archiveMap.put(linked.toUpperCase(), file);
                        }
                    }
                }
            }

            ProcessedFileEntry entry = new ProcessedFileEntry();
            entry.setCustomerId(customerId);
            entry.setAccountNumber(accountNumber);

            List<String> statuses = new ArrayList<>();
            boolean hasSuccess = false;

            for (String type : List.of("EMAIL", "MOBSTAT", "PRINT")) {
                SummaryProcessedFile delivery = methodMap.get(type);
                SummaryProcessedFile archive = archiveMap.get(type);

                String deliveryStatus = null, archiveStatus = null;

                if (delivery != null) {
                    String url = delivery.getBlobURL();
                    deliveryStatus = delivery.getStatus();
                    String reason = delivery.getStatusDescription();

                    switch (type) {
                        case "EMAIL" -> {
                            entry.setPdfEmailFileUrl(url);
                            entry.setPdfEmailFileUrlStatus(deliveryStatus);
                            if ("FAILED".equalsIgnoreCase(deliveryStatus)) entry.setReason(reason);
                        }
                        case "MOBSTAT" -> {
                            entry.setPdfMobstatFileUrl(url);
                            entry.setPdfMobstatFileUrlStatus(deliveryStatus);
                            if ("FAILED".equalsIgnoreCase(deliveryStatus)) entry.setReason(reason);
                        }
                        case "PRINT" -> {
                            entry.setPrintFileUrl(url);
                            entry.setPrintFileUrlStatus(deliveryStatus);
                            if ("FAILED".equalsIgnoreCase(deliveryStatus)) entry.setReason(reason);
                        }
                    }

                    if ("SUCCESS".equalsIgnoreCase(deliveryStatus)) hasSuccess = true;
                }

                if (archive != null) {
                    archiveStatus = archive.getStatus();
                    String aUrl = archive.getBlobURL();

                    entry.setPdfArchiveFileUrl(aUrl); // shared field
                    entry.setPdfArchiveFileUrlStatus(archiveStatus);

                    if ("FAILED".equalsIgnoreCase(archiveStatus) && entry.getReason() == null) {
                        entry.setReason(archive.getStatusDescription());
                    }

                    if ("SUCCESS".equalsIgnoreCase(archiveStatus)) hasSuccess = true;
                }

                // Record method-level status only if at least one file is present
                if (deliveryStatus != null || archiveStatus != null) {
                    if ("SUCCESS".equalsIgnoreCase(deliveryStatus) && "SUCCESS".equalsIgnoreCase(archiveStatus)) {
                        statuses.add("SUCCESS");
                    } else if ("FAILED".equalsIgnoreCase(deliveryStatus) && "FAILED".equalsIgnoreCase(archiveStatus)) {
                        statuses.add("FAILED");
                    } else {
                        statuses.add("PARTIAL");
                    }
                }
            }

            // ✅ Skip if no file for this customer is SUCCESS
            if (!hasSuccess) continue;

            // ✅ Determine overall status
            if (statuses.stream().allMatch(s -> "SUCCESS".equals(s))) {
                entry.setOverAllStatusCode("SUCCESS");
            } else if (statuses.stream().allMatch(s -> "FAILED".equals(s))) {
                entry.setOverAllStatusCode("FAILED");
            } else {
                entry.setOverAllStatusCode("PARTIAL");
            }

            finalList.add(entry);
        }

        return finalList;
    }
}
