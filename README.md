package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.core.JsonProcessingException;
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

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;
import static com.nedbank.kafka.filemanage.constants.AppConstants.*;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

/**
 * KafkaListenerService is responsible for:
 * - Listening to Kafka input topic for file batch processing messages
 * - Downloading blob files to local mount path
 * - Triggering Orchestration APIs (OT) like Debtman/MFC
 * - Waiting for generated output (STD XML), parsing error report and customer summaries
 * - Uploading processed files and generating summary.json
 * - Publishing final response message to Kafka output topic
 *
 * This service acts as an orchestrator between Kafka, Blob Storage, OT system, and summary file generation.
 */
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

    @Value("${ot.runtime.url}")
    private String runtimeBaseUrl;

    @Value("${kafka.topic.audit}")
    private String auditTopic;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final BlobStorageService blobStorageService;
    private final KafkaTemplate<String, String> kafkaTemplate;       // For regular topic
    //private final KafkaTemplate<String, String> auditKafkaTemplate;  // For audit topic
    private final SourceSystemProperties sourceSystemProperties;
    private final RestTemplate restTemplate = new RestTemplate();
    private final ExecutorService executor = Executors.newFixedThreadPool(5);

    @Autowired
    public KafkaListenerService(
            BlobStorageService blobStorageService,
            @Qualifier("kafkaTemplate") KafkaTemplate<String, String> kafkaTemplate,                       // default template
            //@Qualifier("auditKafkaTemplate") KafkaTemplate<String, String> auditKafkaTemplate,  // audit template
            SourceSystemProperties sourceSystemProperties
    ) {
        this.blobStorageService = blobStorageService;
        this.kafkaTemplate = kafkaTemplate;
        //this.auditKafkaTemplate = auditKafkaTemplate;
        this.sourceSystemProperties = sourceSystemProperties;
    }

    /**
     * Kafka consumer method to handle messages from input topic.
     * Performs validation on message structure, downloads files,
     * and triggers orchestration API.
     *
     * @param rawMessage Raw Kafka message in JSON string format
     * @param ack        Kafka acknowledgment to commit offset manually
     */
    @KafkaListener(topics = "${kafka.topic.input}", groupId = "${kafka.consumer.group.id}")
    public void onKafkaMessage(String rawMessage, Acknowledgment ack) {
        String batchId = "";
        try {
            logger.info("📩 [batchId: unknown] Received Kafka message: {}", rawMessage);
            KafkaMessage message = objectMapper.readValue(rawMessage, KafkaMessage.class);
            batchId = message.getBatchId();
            List<BatchFile> batchFiles = message.getBatchFiles();
            if (batchFiles == null || batchFiles.isEmpty()) {
                logger.error("❌ [batchId: {}] Rejected - Empty BatchFiles", batchId);
                ack.acknowledge();
                return;
            }

            long dataCount = batchFiles.stream()
                    .filter(f -> FILE_TYPE_DATA.equalsIgnoreCase(f.getFileType()))
                    .count();
            long refCount = batchFiles.stream()
                    .filter(f -> FILE_TYPE_REF.equalsIgnoreCase(f.getFileType()))
                    .count();

            if (dataCount == 1 && refCount == 0) {
                logger.info("✅ [batchId: {}] Valid with 1 DATA file", batchId);
            } else if (dataCount > 1) {
                logger.error("❌ [batchId: {}] Rejected - Multiple DATA files", batchId);
                ack.acknowledge();
                return;
            } else if (dataCount == 0 && refCount > 0) {
                logger.error("❌ [batchId: {}] Rejected - Only REF files", batchId);
                ack.acknowledge();
                return;
            } else if (dataCount == 1 && refCount > 0) {
                logger.info("✅ [batchId: {}] Valid with DATA + REF files (both will be passed to OT)", batchId);
                message.setBatchFiles(batchFiles);
            } else {
                logger.error("❌ [batchId: {}] Rejected - Invalid or unsupported file type combination", batchId);
                ack.acknowledge();
                return;
            }

            String sanitizedBatchId = batchId.replaceAll(FILENAME_SANITIZE_REGEX, REPLACEMENT_UNDERSCORE);
            String sanitizedSourceSystem = message.getSourceSystem().replaceAll(FILENAME_SANITIZE_REGEX, REPLACEMENT_UNDERSCORE);

            Path batchDir = Paths.get(mountPath, INPUT_FOLDER, sanitizedSourceSystem, sanitizedBatchId);
            if (Files.exists(batchDir)) {
                logger.warn("⚠️ [batchId: {}] Directory already exists at path: {}", batchId, batchDir);
                try (Stream<Path> files = Files.walk(batchDir)) {
                    files.sorted(Comparator.reverseOrder())
                            .map(Path::toFile)
                            .forEach(File::delete);
                    logger.info("🧹 [batchId: {}] Cleaned existing input directory: {}", batchId, batchDir);
                } catch (IOException e) {
                    logger.error("❌ [batchId: {}] Failed to clean directory {} - {}", batchId, batchDir, e.getMessage(), e);
                    throw e;
                }
            }

            Files.createDirectories(batchDir);
            logger.info("📁 [batchId: {}] Created input directory: {}", batchId, batchDir);

            // Download files
            for (BatchFile file : message.getBatchFiles()) {
                String blobUrl = file.getBlobUrl();
                Path localPath = batchDir.resolve(file.getFilename());

                try {
                    if (Files.exists(localPath)) {
                        logger.warn("♻️ [batchId: {}] File already exists, overwriting: {}", batchId, localPath);
                        Files.delete(localPath);
                    }

                    blobStorageService.downloadFileToLocal(blobUrl, localPath);

                    if (!Files.exists(localPath)) {
                        logger.error("❌ [batchId: {}] File missing after download: {}", batchId, localPath);
                        throw new IOException("Download failed for: " + localPath);
                    }

                    file.setBlobUrl(localPath.toString());
                    logger.info("⬇️ [batchId: {}] Downloaded file: {} to {}", batchId, blobUrl, localPath);

                } catch (Exception e) {
                    logger.error("❌ [batchId: {}] Failed to download file: {} - {}", batchId, file.getFilename(), e.getMessage(), e);
                    throw e;
                }
            }

            // ✅ Commit main topic early (before sending INBOUND audit)
            ack.acknowledge();

            // Send INBOUND audit after commit
            Instant startTime = Instant.now();
            long customerCount = 0;
            for(int i=0; i< message.getBatchFiles().size(); i++) {
                customerCount = message.getBatchFiles().get(i).getCustomerCount();
            }
            AuditMessage inboundAudit = buildAuditMessage(message, startTime, startTime,
                    "FmConsume", message.getEventType(), customerCount);
            sendToAuditTopic(inboundAudit);

            // Dynamic lookup for orchestration
            Optional<SourceSystemProperties.SystemConfig> matchingConfig =
                    sourceSystemProperties.getConfigForSourceSystem(sanitizedSourceSystem);

            if (matchingConfig.isEmpty()) {
                logger.error("❌ [batchId: {}] Unsupported or unconfigured source system '{}'", batchId, sanitizedSourceSystem);
                return;
            }

            SourceSystemProperties.SystemConfig config = matchingConfig.get();
            String url = config.getUrl();
            String secretName = sourceSystemProperties.getSystems().get(0).getToken();
            String token = blobStorageService.getSecret(secretName);

            if (url == null || url.isBlank()) {
                logger.error("❌ [batchId: {}] Orchestration URL not configured for source system '{}'", batchId, sanitizedSourceSystem);
                return;
            }

            String finalBatchId = batchId;
            long finalCustomerCount = customerCount;
            executor.submit(() -> {
                Instant otStartTime = Instant.now();
                try {
                    logger.info("🚀 [batchId: {}] Calling Orchestration API: {}", finalBatchId, url);
                    OTResponse otResponse = callOrchestrationBatchApi(token, url, message);
                    logger.info("📤 [batchId: {}] OT request sent successfully", finalBatchId);
                    processAfterOT(message, otResponse);

                    // Send OUTBOUND audit (commit is already done earlier)
                    Instant otEndTime = Instant.now();
                    AuditMessage outboundAudit = buildAuditMessage(message, otStartTime, otEndTime,
                            "Fmcomplete", message.getEventType(), finalCustomerCount);
                    sendToAuditTopic(outboundAudit);

                } catch (Exception ex) {
                    logger.error("❌ [batchId: {}] Error during async OT or post-processing: {}", finalBatchId, ex.getMessage(), ex);
                }
            });

        } catch (Exception ex) {
            logger.error("❌ [batchId: {}] Kafka message processing failed. Error: {}", batchId, ex.getMessage(), ex);
            ack.acknowledge();
        }
    }

    /**
     * Parses the STD XML file to extract customer delivery status information.
     *
     * @param xmlFile  STD Delivery XML file
     * @param errorMap ErrorReport map to determine status (SUCCESS/PARTIAL/FAILED)
     * @return List of CustomerSummary objects
     */
    private List<CustomerSummary> parseSTDXml(File xmlFile, Map<String, Map<String, String>> errorMap) {
        List<CustomerSummary> customerSummaries = new ArrayList<>();

        try {
            DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            Document document = builder.parse(xmlFile);
            document.getDocumentElement().normalize();

            NodeList customerNodes = document.getElementsByTagName("customer");

            for (int i = 0; i < customerNodes.getLength(); i++) {
                Element customerElement = (Element) customerNodes.item(i);

                String accountNumber = null;
                String cisNumber = null;
                List<String> deliveryMethods = new ArrayList<>();

                NodeList keyNodes = customerElement.getElementsByTagName("key");
                for (int j = 0; j < keyNodes.getLength(); j++) {
                    Element keyElement = (Element) keyNodes.item(j);
                    String keyName = keyElement.getAttribute("name");

                    // ✅ Match even if keyName has suffix (_MFC, _DEBITMAN, etc.)
                    if (keyName != null && keyName.toLowerCase().startsWith("accountnumber")) {
                        accountNumber = keyElement.getTextContent();
                    } else if (keyName != null && keyName.toLowerCase().startsWith("cisnumber")) {
                        cisNumber = keyElement.getTextContent();
                    }
                }

                NodeList queueNodes = customerElement.getElementsByTagName("queueName");
                for (int q = 0; q < queueNodes.getLength(); q++) {
                    String method = queueNodes.item(q).getTextContent().trim().toUpperCase();
                    if (!method.isEmpty()) {
                        deliveryMethods.add(method);
                    }
                }

                if (accountNumber != null && cisNumber != null) {
                    CustomerSummary summary = new CustomerSummary();
                    summary.setAccountNumber(accountNumber);
                    summary.setCisNumber(cisNumber);
                    summary.setCustomerId(accountNumber);

                    Map<String, String> deliveryStatusMap = errorMap.getOrDefault(accountNumber, new HashMap<>());
                    summary.setDeliveryStatus(deliveryStatusMap);

                    long failedCount = deliveryMethods.stream()
                            .filter(method -> "FAILED".equalsIgnoreCase(deliveryStatusMap.getOrDefault(method, "")))
                            .count();

                    if (failedCount == deliveryMethods.size()) {
                        summary.setStatus("FAILED");
                    } else if (failedCount > 0) {
                        summary.setStatus("PARTIAL");
                    } else {
                        summary.setStatus("SUCCESS");
                    }

                    customerSummaries.add(summary);

                    logger.debug("📋 Customer: {}, CIS: {}, Methods: {}, Failed: {}, FinalStatus: {}",
                            accountNumber, cisNumber, deliveryMethods, failedCount, summary.getStatus());
                }
            }

        } catch (Exception e) {
            logger.error("❌ Failed parsing STD XML file: {}", xmlFile.getAbsolutePath(), e);
            throw new RuntimeException("Failed to parse XML file: " + xmlFile.getName(), e);
        }

        return customerSummaries;
    }

    /**
     * Performs all post-orchestration processing like:
     * - Waiting for generated STD XML
     * - Parsing error report and STD XML
     * - Uploading output files and building processed files list
     * - Writing and uploading summary.json
     * - Sending Kafka output with summary response
     *
     * @param message     Kafka input message object
     * @param otResponse  OT job response containing jobId and id
     */
    private void processAfterOT(KafkaMessage message, OTResponse otResponse) {
        String batchId = message.getBatchId(); // golden thread
        try {
            logger.info("[{}] ⏳ Waiting for XML for jobId={}, id={}", batchId, otResponse.getJobId(), otResponse.getId());
            File xmlFile = waitForXmlFile(otResponse.getJobId(), otResponse.getId());
            if (xmlFile == null) throw new IllegalStateException("XML not found");

            logger.info("[{}] ✅ Found XML file: {}", batchId, xmlFile);

            Map<String, Map<String, String>> errorMap = parseErrorReport(message);
            logger.info("[{}] 🧾 Parsed error report with {} entries", batchId, errorMap.size());

            List<CustomerSummary> customerSummaries = parseSTDXml(xmlFile, errorMap);
            logger.info("[{}] 📊 Total customerSummaries parsed: {}", batchId, customerSummaries.size());

            List<SummaryProcessedFile> customerList = customerSummaries.stream()
                    .map(cs -> {
                        SummaryProcessedFile spf = new SummaryProcessedFile();
                        spf.setAccountNumber(cs.getAccountNumber());
                        spf.setCustomerId(cs.getCisNumber());
                        return spf;
                    })
                    .collect(Collectors.toList());

            Path jobDir = Paths.get(mountPath, OUTPUT_FOLDER, message.getSourceSystem(), otResponse.getJobId());
            logger.info("[{}] 📂 Resolved jobDir path = {}", batchId, jobDir.toAbsolutePath());
            logger.info("[{}] 🔄 Invoking buildDetailedProcessedFiles...", batchId);
            List<SummaryProcessedFile> processedFiles =
                    buildDetailedProcessedFiles(jobDir, customerList, errorMap, message);
            logger.info("[{}] 📦 Processed {} customer records", batchId, processedFiles.size());

            List<PrintFile> printFiles = uploadPrintFiles(jobDir, message);
            logger.info("[{}] 🖨️ Uploaded {} print files", batchId, printFiles.size());

            String mobstatTriggerUrl = findAndUploadMobstatTriggerFile(jobDir, message);
            logger.info("[{}] 📱 Found Mobstat URL: {}", batchId, mobstatTriggerUrl);

            Map<String, Integer> summaryCounts = extractSummaryCountsFromXml(xmlFile);

            String allFileNames = message.getBatchFiles().stream()
                    .map(BatchFile::getFilename)
                    .collect(Collectors.joining(", "));

            SummaryPayload payload = SummaryJsonWriter.buildPayload(
                    message, processedFiles, allFileNames, batchId,
                    String.valueOf(message.getTimestamp()), errorMap, printFiles
            );

            if (payload.getHeader() != null) {
                payload.getHeader().setTimestamp(String.valueOf(message.getTimestamp()));
            }

            String fileName = SUMMARY_FILENAME_PREFIX + batchId + JSON_EXTENSION;
            String summaryPath = SummaryJsonWriter.writeSummaryJsonToFile(payload);
            String summaryUrl = blobStorageService.uploadSummaryJson(summaryPath, message, fileName);
            payload.setSummaryFileURL(decodeUrl(summaryUrl));
            logger.info("[{}] 📁 Summary JSON uploaded to: {}", batchId, decodeUrl(summaryUrl));

            logger.info("[{}] 📄 Final Summary Payload:\n{}", batchId,
                    objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(payload));

            SummaryResponse response = new SummaryResponse();
            response.setBatchID(batchId);
            response.setFileName(payload.getFileName());
            response.setHeader(payload.getHeader());
            response.setMetadata(payload.getMetadata());
            response.setPayload(payload.getPayload());
            response.setSummaryFileURL(decodeUrl(summaryUrl));
            response.setTimestamp(String.valueOf(message.getTimestamp()));

            kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(
                    new ApiResponse("Summary generated", "COMPLETED", response)));

            logger.info("[{}] ✅ Kafka output sent with response: {}", batchId,
                    objectMapper.writeValueAsString(response));

        } catch (Exception e) {
            logger.error("[{}] ❌ Error post-OT summary generation: {}", batchId, e.getMessage(), e);
        }
    }

    /**
     * Looks for a `.trigger` file in the output job directory and uploads it if found.
     *
     * @param jobDir  Path to the job output directory
     * @param message Kafka input message
     * @return Blob URL of uploaded trigger file or null if not found
     */
    private String findAndUploadMobstatTriggerFile(Path jobDir, KafkaMessage message) {
        // ✅ First check if the directory exists and is actually a folder
        if (jobDir == null || !Files.exists(jobDir) || !Files.isDirectory(jobDir)) {
            logger.warn("⚠️ MOBSTAT job directory does not exist or is not a directory: {}", jobDir);
            return null; // Skip gracefully
        }

        try (Stream<Path> stream = Files.list(jobDir)) {
            Optional<Path> trigger = stream
                    .filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().toLowerCase().endsWith(TRIGGER_FILE_EXTENSION))
                    .findFirst();

            if (trigger.isPresent()) {
                Path triggerFile = trigger.get();
                try {
                    String blobUrl = blobStorageService.uploadFile(
                            triggerFile.toFile(),
                            String.format(MOBSTAT_TRIGGER_UPLOAD_PATH_FORMAT,
                                    message.getSourceSystem(), message.getBatchId(),
                                    message.getUniqueConsumerRef(), triggerFile.getFileName())
                    );

                    logger.info("📤 Uploaded MOBSTAT trigger file: {} -> {}", triggerFile, blobUrl);
                    return decodeUrl(blobUrl);
                } catch (Exception uploadEx) {
                    logger.error("⚠️ Failed to upload MOBSTAT trigger file: {}", triggerFile, uploadEx);
                    // Continue anyway
                    return null;
                }
            } else {
                logger.warn("⚠️ No .trigger file found in MOBSTAT job directory: {}", jobDir);
                return null;
            }

        } catch (IOException e) {
            logger.error("⚠️ Error scanning for .trigger file in jobDir: {}", jobDir, e);
            return null; // Continue gracefully
        }
    }

    /**
     * Invokes the external OT orchestration batch API.
     *
     * @param token Authentication token
     * @param url   API endpoint
     * @param msg   Kafka input message payload
     * @return OTResponse containing jobId and id
     */
    private OTResponse callOrchestrationBatchApi(String token, String url, KafkaMessage msg) {
        OTResponse otResponse = new OTResponse();
        try {
            logger.info("📡 Initiating OT orchestration call to URL: {} for batchId: {} and sourceSystem: {}",
                    url, msg.getBatchId(), msg.getSourceSystem());

            HttpHeaders headers = new HttpHeaders();
            headers.set(HEADER_AUTHORIZATION, BEARER_PREFIX + token);
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<String> request = new HttpEntity<>(objectMapper.writeValueAsString(msg), headers);
            logger.debug("📨 OT Request Payload: {}", objectMapper.writeValueAsString(msg));

            ResponseEntity<Map> response = restTemplate.exchange(url, HttpMethod.POST, request, Map.class);
            logger.info("✅ Received OT response with status: {} for batchId: {}",
                    response.getStatusCode(), msg.getBatchId());

            List<Map<String, Object>> data =
                    (List<Map<String, Object>>) response.getBody().get(OT_RESPONSE_DATA_KEY);

            if (data != null && !data.isEmpty()) {
                Map<String, Object> item = data.get(0);
                otResponse.setJobId((String) item.get(OT_JOB_ID_KEY));
                otResponse.setId((String) item.get(OT_ID_KEY));
                msg.setJobName(otResponse.getJobId());

                logger.info("🎯 OT Job created successfully - JobID: {}, ID: {}, BatchID: {}",
                        otResponse.getJobId(), otResponse.getId(), msg.getBatchId());

                // 🔄 Poll runtime API until status=complete
                String runtimeUrl = runtimeBaseUrl + otResponse.getJobId();

                boolean completed = false;
                int maxRetries = 60;              // up to 30 minutes if poll interval = 30s
                long pollIntervalMillis = 30_000; // 30 seconds
                int retryCount = 0;

                while (!completed && retryCount < maxRetries) {
                    try {
                        ResponseEntity<Map> runtimeResponse = restTemplate.exchange(
                                runtimeUrl, HttpMethod.GET, new HttpEntity<>(headers), Map.class);

                        Map<String, Object> runtimeBody = runtimeResponse.getBody();
                        if (runtimeBody != null && "success".equals(runtimeBody.get("status"))) {
                            Object dataObj = runtimeBody.get("data");

                            if (dataObj instanceof Map) {
                                Map<String, Object> runtimeData = (Map<String, Object>) dataObj;
                                String jobStatus = (String) runtimeData.get("status");

                                logger.info("🔎 Runtime check attempt {} for JobID {} => status={}",
                                        retryCount + 1, otResponse.getJobId(), jobStatus);

                                if ("complete".equalsIgnoreCase(jobStatus)) {
                                    logger.info("✅ Runtime job completed for JobID: {} (BatchID: {})",
                                            otResponse.getJobId(), msg.getBatchId());
                                    completed = true;
                                    otResponse.setSuccess(true);
                                    break;
                                }
                            } else {
                                logger.error("❌ Unexpected runtime response format: data is not a Map (JobID={})",
                                        otResponse.getJobId());
                            }
                        }
                    } catch (Exception ex) {
                        logger.warn("⚠️ Runtime status check failed for JobID {} - {}",
                                otResponse.getJobId(), ex.getMessage());
                    }

                    retryCount++;
                    Thread.sleep(pollIntervalMillis);
                }

                if (!completed) {
                    String errMsg = String.format(
                            "❌ Job did not complete within %d minutes (JobID=%s, BatchID=%s)",
                            (maxRetries * pollIntervalMillis) / 60000,
                            otResponse.getJobId(),
                            msg.getBatchId()
                    );
                    logger.error(errMsg);
                    throw new RuntimeException(errMsg); // 🚨 discard flow
                }

            } else {
                logger.error("❌ No data found in OT orchestration response for batchId: {}", msg.getBatchId());
                otResponse.setSuccess(false);
                otResponse.setMessage(NO_OT_DATA_MESSAGE);
            }

            return otResponse;

        } catch (Exception e) {
            logger.error("❌ Exception during OT orchestration call for batchId: {} - {}",
                    msg.getBatchId(), e.getMessage(), e);
            otResponse.setSuccess(false);
            otResponse.setMessage(OT_CALL_FAILURE_PREFIX + e.getMessage());
            return otResponse;
        }
    }

    /**
     * Waits for the STD delivery XML file to be generated in job directory.
     * Ensures the file is stable (not still being written).
     *
     * @param jobId OT job ID
     * @param id    OT sub-job ID
     * @return File object for the found XML or null if timeout
     * @throws InterruptedException If thread sleep is interrupted
     */

    private File waitForXmlFile(String jobId, String id) throws InterruptedException {
        Path docgenRoot = Paths.get(mountPath, "jobs", jobId, id, DOCGEN_FOLDER);
        long startTime = System.currentTimeMillis();

        while ((System.currentTimeMillis() - startTime) < rptMaxWaitSeconds * 1000L) {
            if (Files.exists(docgenRoot)) {
                try (Stream<Path> paths = Files.walk(docgenRoot)) {
                    Optional<Path> xmlPathOpt = paths
                            .filter(Files::isRegularFile)
                            .filter(p -> p.getFileName().toString().equalsIgnoreCase(XML_FILE_NAME))
                            .findFirst();

                    if (xmlPathOpt.isPresent()) {
                        File xmlFile = xmlPathOpt.get().toFile();
                        logger.info("[{}] 📄 Found generated XML file: {}", jobId, xmlFile.getAbsolutePath());
                        return xmlFile;
                    }
                } catch (IOException e) {
                    logger.warn(LOG_ERROR_SCANNING_FOLDER, jobId, id, e.getMessage(), e);
                }
            } else {
                logger.debug(LOG_DOCGEN_FOLDER_NOT_FOUND, jobId, id, docgenRoot);
            }

            TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
        }

        String errMsg = String.format(LOG_XML_TIMEOUT, docgenRoot, jobId, id);
        logger.error(errMsg);
        throw new IllegalStateException(errMsg);
    }

    /**
     * Parses ErrorReport.csv file under the job directory and maps delivery method status
     *
     * @param msg Kafka message to locate job folder
     * @return Map of accountNumber -> (method -> status)
     */
    private Map<String, Map<String, String>> parseErrorReport(KafkaMessage msg) {
        Map<String, Map<String, String>> result = new HashMap<>();
        try {
            String jobRootPath = mountPath + "/jobs";
            Path jobRoot = Paths.get(jobRootPath);
            String jobId = msg.getJobName();

            // Find all ErrorReport.csv files under this job's path
            Path jobPath = jobRoot.resolve(jobId);
            if (!Files.exists(jobPath)) {
                logger.warn("❌ Job path not found: {}", jobPath);
                return result;
            }

            logger.info("🔍 Searching ErrorReport.csv under: {}", jobPath);

            try (Stream<Path> stream = Files.walk(jobPath)) {
                Optional<Path> reportFile = stream
                        .filter(path -> path.getFileName().toString().equalsIgnoreCase(ERROR_REPORT_FILE_NAME))
                        .findFirst();

                if (reportFile.isEmpty()) {
                    logger.warn("⚠️ ErrorReport.csv not found under job {}", jobId);
                    return result;
                }

                Path reportPath = reportFile.get();
                logger.info("✅ Found ErrorReport.csv at: {}", reportPath);

                try (BufferedReader reader = Files.newBufferedReader(reportPath)) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        String[] parts = line.split("\\|");

                        if (parts.length >= 3) {
                            String account = parts[ERROR_REPORT_INDEX_ACCOUNT].trim();

                            String method = DEFAULT_METHOD;
                            if (parts.length >= 4) {
                                method = parts[ERROR_REPORT_INDEX_METHOD_V1].trim();
                            } else if (parts.length >= 3) {
                                method = parts[ERROR_REPORT_INDEX_METHOD_V2].trim();
                            }

                            String status = DEFAULT_STATUS;
                            if (parts.length >= 5) {
                                status = parts[ERROR_REPORT_INDEX_STATUS_V1].trim();
                            } else if (parts.length >= 4) {
                                status = parts[ERROR_REPORT_INDEX_STATUS_V2].trim();
                            }

                            if (method.isEmpty()) method = DEFAULT_METHOD;
                            if (status.isEmpty()) status = DEFAULT_STATUS;

                            result.computeIfAbsent(account, k -> new HashMap<>()).put(method.toUpperCase(), status);
                        }
                    }
                } catch (IOException e) {
                    logger.error("❌ Error reading ErrorReport.csv: {}", e.getMessage(), e);
                }
            }
        } catch (Exception ex) {
            logger.error("❌ Failed to parse error report: {}", ex.getMessage(), ex);
        }

        return result;
    }

    /**
     * Builds a list of SummaryProcessedFile entries with archive/output blob URLs
     * and delivery status for each customer (EMAIL, MOBSTAT, PRINT).
     *
     * @param jobDir        Output job directory
     * @param customerList  Customer basic summary list
     * @param errorMap      Error report map (account -> method -> status)
     * @param msg           Kafka message for path info
     * @return List of SummaryProcessedFile objects with blob URLs and status
     */
    private List<SummaryProcessedFile> buildDetailedProcessedFiles(
            Path jobDir,
            List<SummaryProcessedFile> customerList,
            Map<String, Map<String, String>> errorMap,
            KafkaMessage msg) throws IOException {

        List<SummaryProcessedFile> finalList = new ArrayList<>();
        if (jobDir == null || customerList == null || msg == null) return finalList;

        // Maps for each type
        Map<String, Map<String, String>> accountToArchiveFiles = new HashMap<>();
        Map<String, Map<String, List<String>>> accountToEmailFiles = new HashMap<>();
        Map<String, Map<String, String>> accountToMobstatFiles = new HashMap<>();
        Map<String, Map<String, String>> accountToPrintFiles = new HashMap<>();

        // Walk ALL folders inside jobDir
        try (Stream<Path> stream = Files.walk(jobDir)) {
            stream.filter(Files::isRegularFile).forEach(file -> {
                if (!Files.exists(file)) {
                    logger.warn("[{}] ⏩ Skipping missing file: {}", msg.getBatchId(), file);
                    return;
                }

                String fileName = file.getFileName().toString().toLowerCase();
                String parentFolder = file.getParent().getFileName().toString().toLowerCase();

                // Only allow PDF, PS, HTML, TXT
                if (!(fileName.endsWith(".pdf") || fileName.endsWith(".ps") ||
                        fileName.endsWith(".html") || fileName.endsWith(".txt"))) {
                    logger.debug("[{}] ⏩ Skipping unsupported file: {}", msg.getBatchId(), fileName);
                    return;
                }

                try {
                    // ✅ Upload only if folder is exactly archive/email/mobstat/print
                    if (!(parentFolder.equalsIgnoreCase("archive") ||
                            parentFolder.equalsIgnoreCase("email") ||
                            parentFolder.equalsIgnoreCase("mobstat") ||
                            parentFolder.equalsIgnoreCase("print"))) {
                        logger.info("[{}] ⏩ Ignoring file (outside archive/email/mobstat/print): {} in {}",
                                msg.getBatchId(), fileName, parentFolder);
                        return; // skip upload completely
                    }

                    // Upload now (only for valid folders)
                    String url = decodeUrl(blobStorageService.uploadFileByMessage(file.toFile(), parentFolder, msg));

                    // Match file to customers by account number
                    for (SummaryProcessedFile customer : customerList) {
                        if (customer == null || customer.getAccountNumber() == null) continue;
                        String account = customer.getAccountNumber();
                        if (!fileName.contains(account)) continue;

                        if (parentFolder.equalsIgnoreCase("archive")) {
                            accountToArchiveFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                            logger.info("[{}] 📦 Uploaded archive file={} for account={}, url={}", msg.getBatchId(), fileName, account, url);

                        } else if (parentFolder.equalsIgnoreCase("email")) {
                            Map<String, List<String>> fileMap = accountToEmailFiles.computeIfAbsent(account, k -> new HashMap<>());
                            if (fileName.endsWith(".pdf")) {
                                fileMap.computeIfAbsent("PDF", k -> new ArrayList<>()).add(url);
                            } else if (fileName.endsWith(".html")) {
                                fileMap.computeIfAbsent("HTML", k -> new ArrayList<>()).add(url);
                            } else if (fileName.endsWith(".txt")) {
                                fileMap.computeIfAbsent("TEXT", k -> new ArrayList<>()).add(url);
                            }
                            logger.info("[{}] 📧 Uploaded email file={} for account={}, url={}", msg.getBatchId(), fileName, account, url);

                        } else if (parentFolder.equalsIgnoreCase("mobstat")) {
                            accountToMobstatFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                            logger.info("[{}] 📱 Uploaded mobstat file={} for account={}, url={}", msg.getBatchId(), fileName, account, url);

                        } else if (parentFolder.equalsIgnoreCase("print")) {
                            accountToPrintFiles.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, url);
                            logger.info("[{}] 🖨 Uploaded print file={} for account={}, url={}", msg.getBatchId(), fileName, account, url);
                        }
                    }

                } catch (Exception e) {
                    logger.error("[{}] ⚠️ Failed to upload file {}: {}", msg.getBatchId(), fileName, e.getMessage(), e);
                }
            });
        }

        // Build final list with fixed combinations
        Set<String> uniqueKeys = new HashSet<>();
        for (SummaryProcessedFile customer : customerList) {
            if (customer == null || customer.getAccountNumber() == null) continue;
            String account = customer.getAccountNumber();

            Map<String, String> archivesForAccount = accountToArchiveFiles.getOrDefault(account, Collections.emptyMap());
            Map<String, List<String>> emailsForAccount = accountToEmailFiles.getOrDefault(account, Collections.emptyMap());
            Map<String, String> mobstatsForAccount = accountToMobstatFiles.getOrDefault(account, Collections.emptyMap());

            if (archivesForAccount.isEmpty() && emailsForAccount.isEmpty() && mobstatsForAccount.isEmpty()) continue;

            List<Object> archiveFiles = archivesForAccount.isEmpty() ? Collections.singletonList(null) : new ArrayList<>(archivesForAccount.keySet());
            List<Object> mobstatFiles = mobstatsForAccount.isEmpty() ? Collections.singletonList(null) : new ArrayList<>(mobstatsForAccount.values());
            List<Object> pdfEmails = emailsForAccount.getOrDefault("PDF", Collections.emptyList()).isEmpty() ? Collections.singletonList(null) : new ArrayList<>(emailsForAccount.get("PDF"));
            List<Object> htmlEmails = emailsForAccount.getOrDefault("HTML", Collections.emptyList()).isEmpty() ? Collections.singletonList(null) : new ArrayList<>(emailsForAccount.get("HTML"));
            List<Object> txtEmails = emailsForAccount.getOrDefault("TEXT", Collections.emptyList()).isEmpty() ? Collections.singletonList(null) : new ArrayList<>(emailsForAccount.get("TEXT"));

            for (Object archiveFileNameObj : archiveFiles) {
                for (Object mobstatUrlObj : mobstatFiles) {
                    for (Object pdfEmailObj : pdfEmails) {
                        for (Object htmlEmailObj : htmlEmails) {
                            for (Object txtEmailObj : txtEmails) {

                                String archiveFileName = archiveFileNameObj != null ? archiveFileNameObj.toString() : null;
                                String mobstatUrl = mobstatUrlObj != null ? mobstatUrlObj.toString() : null;
                                String pdfEmail = pdfEmailObj != null ? pdfEmailObj.toString() : null;
                                String htmlEmail = htmlEmailObj != null ? htmlEmailObj.toString() : null;
                                String txtEmail = txtEmailObj != null ? txtEmailObj.toString() : null;

                                String key = customer.getCustomerId() + "|" + account + "|" +
                                        (archiveFileName != null ? archiveFileName : "noArchive") + "|" +
                                        (mobstatUrl != null ? mobstatUrl : "noMobstat") + "|" +
                                        (pdfEmail != null ? pdfEmail : "noPdf") + "|" +
                                        (htmlEmail != null ? htmlEmail : "noHtml") + "|" +
                                        (txtEmail != null ? txtEmail : "noTxt");

                                if (uniqueKeys.contains(key)) continue;
                                uniqueKeys.add(key);

                                SummaryProcessedFile entry = new SummaryProcessedFile();
                                BeanUtils.copyProperties(customer, entry);

                                entry.setArchiveBlobUrl(archiveFileName != null ? archivesForAccount.get(archiveFileName) : null);
                                entry.setPdfMobstatFileUrl(mobstatUrl);
                                entry.setEmailBlobUrlPdf(pdfEmail);
                                entry.setEmailBlobUrlHtml(htmlEmail);
                                entry.setEmailBlobUrlText(txtEmail);

                                finalList.add(entry);
                            }
                        }
                    }
                }
            }
        }

        logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
        return finalList;
    }

    /**
     * Uploads all files under the print directory and creates PrintFile entries.
     *
     * @param jobDir Output job directory
     * @param msg    Kafka message for blob path info
     * @return List of PrintFile objects containing blob URLs
     */
        private List<PrintFile> uploadPrintFiles(Path jobDir, KafkaMessage msg) {
        List<PrintFile> printFiles = new ArrayList<>();

        if (jobDir == null || msg == null || msg.getSourceSystem() == null) {
            logger.error("❌ Invalid input: jobDir={}, msg={}, sourceSystem={}", jobDir, msg, msg != null ? msg.getSourceSystem() : null);
            return printFiles;
        }

        // 1. Check inside print folder (existing logic)
        Path printDir = jobDir.resolve(PRINT_FOLDER_NAME);
        if (Files.exists(printDir)) {
            try (Stream<Path> stream = Files.list(printDir)) {
                stream.filter(Files::isRegularFile).forEach(f -> {
                    try {
                        String fileName = f.getFileName() != null ? f.getFileName().toString() : UNKNOWN_FILE_NAME;
                        String uploadPath = msg.getSourceSystem() + "/" + msg.getBatchId() + "/" + msg.getUniqueConsumerRef() + "/" + PRINT_FOLDER_NAME + "/" + fileName;

                        String blob = blobStorageService.uploadFile(f.toFile(), uploadPath);
                        printFiles.add(new PrintFile(blob));

                        logger.info("📤 Uploaded print file: {} -> {}", fileName, blob);
                    } catch (Exception e) {
                        logger.warn("⚠️ Failed to upload print file: {}", f, e);
                    }
                });
            } catch (IOException e) {
                logger.error("❌ Failed to list files in '{}' directory: {}", PRINT_FOLDER_NAME, printDir, e);
            }
        } else {
            logger.info("ℹ️ No '{}' directory found in jobDir: {}", PRINT_FOLDER_NAME, jobDir);
        }

        // 2. Also check jobDir root for stray .ps files and upload them under print folder
        try (Stream<Path> stream = Files.list(jobDir)) {
            stream.filter(Files::isRegularFile)
                    .filter(f -> f.getFileName() != null && f.getFileName().toString().toLowerCase().endsWith(".ps"))
                    .forEach(f -> {
                        try {
                            String fileName = f.getFileName().toString();
                            String uploadPath = msg.getSourceSystem() + "/" + msg.getBatchId() + "/" + msg.getUniqueConsumerRef() + "/" + PRINT_FOLDER_NAME + "/" + fileName;

                            String blob = blobStorageService.uploadFile(f.toFile(), uploadPath);
                            printFiles.add(new PrintFile(blob));

                            logger.info("📤 Uploaded stray .ps file from root: {} -> {}", fileName, blob);
                        } catch (Exception e) {
                            logger.warn("⚠️ Failed to upload stray .ps file: {}", f, e);
                        }
                    });
        } catch (IOException e) {
            logger.error("❌ Failed to list root directory for stray .ps files: {}", jobDir, e);
        }

        return printFiles;
    }

    /**
     * Extracts customer and page count values from STD XML's outputList node.
     *
     * @param xmlFile STD delivery XML
     * @return Map with "customersProcessed" and "pagesProcessed" as keys
     */
    private Map<String, Integer> extractSummaryCountsFromXml(File xmlFile) {
        Map<String, Integer> summaryCounts = new HashMap<>();

        if (xmlFile == null || !xmlFile.exists() || !xmlFile.canRead()) {
            logger.warn("⚠️ Invalid or unreadable XML file: {}", xmlFile);
            return summaryCounts;
        }

        try {
            Document doc = DocumentBuilderFactory.newInstance()
                    .newDocumentBuilder()
                    .parse(xmlFile);
            doc.getDocumentElement().normalize();

            NodeList outputListNodes = doc.getElementsByTagName("outputList");
            if (outputListNodes.getLength() > 0) {
                Element outputList = (Element) outputListNodes.item(0);
                String customersProcessed = outputList.getAttribute(CUSTOMERS_PROCESSED_KEY);
                String pagesProcessed = outputList.getAttribute(PAGES_PROCESSED_KEY);

                int custCount = (customersProcessed != null && !customersProcessed.isBlank())
                        ? Integer.parseInt(customersProcessed.trim()) : 0;
                int pageCount = (pagesProcessed != null && !pagesProcessed.isBlank())
                        ? Integer.parseInt(pagesProcessed.trim()) : 0;

                summaryCounts.put(CUSTOMERS_PROCESSED_KEY, custCount);
                summaryCounts.put(PAGES_PROCESSED_KEY, pageCount);

                logger.info("📄 Extracted summary counts from {}: customersProcessed={}, pagesProcessed={}",
                        xmlFile.getName(), custCount, pageCount);
            } else {
                logger.info("ℹ️ No <outputList> found in XML: {}", xmlFile.getName());
            }
        } catch (Exception e) {
            logger.warn("⚠️ Unable to extract summary counts from XML file: {}", xmlFile.getName(), e);
        }

        return summaryCounts;
    }

    private AuditMessage buildAuditMessage(KafkaMessage message,
                                           Instant startTime,
                                           Instant endTime,
                                           String serviceName,
                                           String eventType,
                                           long customerCount) {
        AuditMessage audit = new AuditMessage();
        audit.setBatchId(message.getBatchId());
        audit.setServiceName(serviceName);
        audit.setSystemEnv(message.getSystemEnv()); // DEV/QA/PROD
        audit.setSourceSystem(message.getSourceSystem());
        audit.setTenantCode(message.getTenantCode());
        audit.setChannelID(message.getChannelID());
        audit.setProduct(message.getProduct());
        audit.setJobName(message.getJobName());
        audit.setUniqueConsumerRef(message.getUniqueConsumerRef());
        audit.setTimestamp(Instant.now().toString());
        audit.setRunPriority(message.getRunPriority());
        audit.setEventType(eventType);
        audit.setStartTime(startTime.toString());
        audit.setEndTime(endTime.toString());
        audit.setCustomerCount(customerCount);

        List<AuditBatchFile> auditFiles = message.getBatchFiles().stream()
                .map(f -> new AuditBatchFile(f.getBlobUrl(), f.getFilename(), f.getFileType()))
                .toList();
        audit.setBatchFiles(auditFiles);
        return audit;
    }

    /**
     * Updated method using CompletableFuture for Spring Kafka 3.x+
     */
    private void sendToAuditTopic(AuditMessage auditMessage) {
        try {
            String auditJson = objectMapper.writeValueAsString(auditMessage);

            CompletableFuture<SendResult<String, String>> future =
                    kafkaTemplate.send(auditTopic, auditMessage.getBatchId(), auditJson);

            future.whenComplete((result, ex) -> {
                if (ex != null) {
                    logger.error("❌ Failed to send audit message for batchId {}: {}", auditMessage.getBatchId(), ex.getMessage(), ex);
                } else {
                    logger.info("📣 Audit message sent successfully for batchId {}: {}", auditMessage.getBatchId(), auditJson);
                }
            });

        } catch (JsonProcessingException e) {
            logger.error("❌ Failed to serialize audit message for batchId {}: {}", auditMessage.getBatchId(), e.getMessage(), e);
        }
    }

    /**
     * Decodes a URL-encoded string using UTF-8 charset.
     *
     * @param url Encoded URL
     * @return Decoded URL string
     */
    private String decodeUrl(String url) {
        try {
            return URLDecoder.decode(url, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return url;
        }
    }
    /**
     * Gracefully shuts down executor service on bean destruction.
     */
    @PreDestroy
    public void shutdownExecutor() {
        logger.info("⚠️ Shutting down executor service");
        executor.shutdown();
    }
    /**
     * Internal class representing the response from OT orchestration call.
     */
    static class OTResponse {
        private String jobId;
        private String id;
        private boolean success;
        private String message;

        public boolean isSuccess() {
            return success;
        }
        public void setSuccess(boolean success) {
            this.success = success;
        }
        public String getMessage() {
            return message;
        }
        public void setMessage(String message) {
            this.message = message;
        }
        public String getJobId() { return jobId; }
        public void setJobId(String jobId) { this.jobId = jobId; }
        public String getId() { return id; }
        public void setId(String id) { this.id = id; }
    }
}

{
  "datastreamName": "ecp_batch_composition",
  "datastreamType": "logs",
  "batchId": "f6a1bdb5-2fb2-4f77-a7d3-7087758a2572",
  "serviceName": "ecmbatch-archive-listener",
  "systemEnv": "DEV",
  "sourceSystem": "DEBTMAN",
  "tenantCode": "ZANBL",
  "channelID": "1",
  "audienceID": "",
  "product": "DEBTMAN",
  "jobName": "DEBTMAN",
  "consumerRef": "",
  "timestamp": "2025-10-16T08:45:00Z",
  "eventType": "",
  "startTime": "2025-10-16T08:00:00Z",
  "endTime": "2025-10-16T08:30:00Z",
  "customerCount": 150,
  "batchFiles": [
    {
      "blobUrl": "https://nsndvextr01.blob.core.windows.net/nsndevextrm01/DEBTMAN/076f2b3c-37bc-4bcb-ab6a-29041acfc0f…,
      "fileName": "8000013557201_LHSL05.pdf",
      "fileType": "PDF"
    },
    {
      "blobUrl": "https://nsndvextr01.blob.core.windows.net/nsndevextrm01/DEBTMAN/076f2b3c-37bc-4bcb-ab6a-29041acfc0f…,
      "fileName": "8001453741101_LHDL05E.pdf",
      "fileType": "PDF"
    }
  ],
  "success": true,
  "errorCode": "",
  "errorMessage": "",
  "retryFlag": false,
  "retryCount": 0
}


FileManager (Filemanager composition service) to the audit topic
Data element					Value
title	 	 			ECPBatchAudit
type		 			object
properties:					
	datastreamName				Fmcompose
	datastreamType				logs
 	batchId				Get value from event received
 	serviceName				Fmcompose
	systemEnv				DEV/ETE/QA/PROD
	sourceSystem				Get value from event received
	tenantCode				Get value from event received
	channelId				Get value from event received
	audienceId				Get value from event received
	product 				Get value from event received
	jobName				Get value from event – links to composition application
	consumerRef				Get value from event received
	timestamp				
	eventType				
	startTime				
	endTime				
	customerCount				
	batchFiles:				
		type			
		Items:	type		
			Properties:		
				bobUrl	Where data file has been copied to on azure blob storage - Get value from event received
				fileName	Get value from event received
				fileType	Get value from event received
					
	success				True/false
	errorCode				
	errorMessage				
	retryFlag				
	retryCount				
					
					


FileManager (Filemanager service complete) to the audit topic
Data element					Value
title	 	 			ECPBatchAudit
type		 			object
Properties:					
	datastreamName				Fmcomplete
	datastreamType				logs
 	batchId				Get value from event received
 	serviceName				Fmcomplete
	systemEnv				DEV/ETE/QA/PROD
	sourceSystem				Get value from event received
	tenantCode				Get value from event received
	channelId				Get value from event received
	audienceId				Get value from event received
	product 				Get value from event received
	jobName				Get value from event received
	consumerRef				Get value from event received
	timestamp				
	eventType				
	startTime				
	endTime				
	customerCount				
	batchFiles:				
		type			
		Items:	type		
			Properties:		
				blobUrl	Where summary file has been copied to on azure blob storage 
				fileName	Summary report name
				fileType	
					
	success				True/false
	errorCode				
	errorMessage				
	retryFlag				
	retryCount				
					
					===================================

					package com.nedbank.kafka.filemanage.model;

import lombok.*;
import java.time.Instant;
import java.util.List;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class ECPBatchAudit {
    private String title = "ECPBatchAudit";
    private String type = "object";
    private String datastreamName;
    private String datastreamType = "logs";
    private String batchId;
    private String serviceName;
    private String systemEnv;
    private String sourceSystem;
    private String tenantCode;
    private String channelId;
    private String audienceId;
    private String product;
    private String jobName;
    private String consumerRef;
    private String timestamp;
    private String eventType;
    private Instant startTime;
    private Instant endTime;
    private long customerCount;
    private List<BatchFileAudit> batchFiles;
    private boolean success;
    private String errorCode;
    private String errorMessage;
    private boolean retryFlag;
    private int retryCount;

    @Data
    @AllArgsConstructor
    @NoArgsConstructor
    @Builder
    public static class BatchFileAudit {
        private String blobUrl;
        private String fileName;
        private String fileType;
    }
}
=====
package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.*;
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

import java.io.*;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;
import static com.nedbank.kafka.filemanage.constants.AppConstants.*;

@Service
public class KafkaListenerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    @Value("${mount.path}")
    private String mountPath;

    @Value("${kafka.topic.output}")
    private String kafkaOutputTopic;

    @Value("${kafka.topic.audit}")
    private String auditTopic;

    @Value("${rpt.max.wait.seconds}")
    private int rptMaxWaitSeconds;

    @Value("${rpt.poll.interval.millis}")
    private int rptPollIntervalMillis;

    @Value("${ot.runtime.url}")
    private String runtimeBaseUrl;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final BlobStorageService blobStorageService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final SourceSystemProperties sourceSystemProperties;
    private final RestTemplate restTemplate = new RestTemplate();
    private final ExecutorService executor = Executors.newFixedThreadPool(5);

    @Autowired
    public KafkaListenerService(
            BlobStorageService blobStorageService,
            @Qualifier("kafkaTemplate") KafkaTemplate<String, String> kafkaTemplate,
            SourceSystemProperties sourceSystemProperties) {
        this.blobStorageService = blobStorageService;
        this.kafkaTemplate = kafkaTemplate;
        this.sourceSystemProperties = sourceSystemProperties;
    }

    @KafkaListener(topics = "${kafka.topic.input}", groupId = "${kafka.consumer.group.id}")
    public void onKafkaMessage(String rawMessage, Acknowledgment ack) {
        String batchId = "";
        try {
            KafkaMessage message = objectMapper.readValue(rawMessage, KafkaMessage.class);
            batchId = message.getBatchId();
            Instant startTime = Instant.now();

            logger.info("📩 Received Message for BatchId: {}", batchId);
            ack.acknowledge();

            // send Fmcompose audit
            sendFmComposeAudit(message, startTime);

            // Execute orchestration asynchronously with retry tracking
            executor.submit(() -> {
                Instant otStart = Instant.now();
                OTResponse otResponse = null;
                boolean retryFlag = false;
                int retryCount = 0;
                int maxRetries = 3;

                while (retryCount <= maxRetries) {
                    try {
                        otResponse = callOrchestrationBatchApi("dummy-token", runtimeBaseUrl, message);
                        if (otResponse.isSuccess()) break;
                    } catch (Exception e) {
                        logger.warn("Orchestration call failed (attempt {}): {}", retryCount + 1, e.getMessage());
                    }
                    retryCount++;
                    if (retryCount > 0) retryFlag = true;
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ie) {
                        Thread.currentThread().interrupt();
                    }
                }

                if (otResponse == null) {
                    otResponse = new OTResponse();
                    otResponse.setSuccess(false);
                    otResponse.setMessage("Failed after retries");
                }

                try {
                    processAfterOT(message, otResponse);
                    Instant otEnd = Instant.now();
                    String summaryUrl = mountPath + "/summary/" + batchId + ".json";

                    // send Fmcomplete audit with dynamic retry info
                    sendFmCompleteAudit(message, summaryUrl, otStart, otEnd, otResponse.isSuccess(), retryFlag, retryCount);

                } catch (Exception e) {
                    logger.error("Error in async orchestration post-processing: {}", e.getMessage(), e);
                    sendFmCompleteAudit(message, null, otStart, Instant.now(), false, retryFlag, retryCount);
                }
            });

        } catch (Exception ex) {
            logger.error("Kafka message processing failed. BatchId={}, Error={}", batchId, ex.getMessage(), ex);
            ack.acknowledge();
        }
    }

    // Audit sending methods with dynamic retry info

    private void sendFmComposeAudit(KafkaMessage message, Instant startTime) {
        ECPBatchAudit audit = ECPBatchAudit.builder()
                .datastreamName("Fmcompose")
                .serviceName("Fmcompose")
                .systemEnv(System.getenv().getOrDefault("SYSTEM_ENV", "DEV"))
                .batchId(message.getBatchId())
                .sourceSystem(message.getSourceSystem())
                .tenantCode(message.getTenantCode())
                .channelId(message.getChannelId())
                .audienceId(message.getAudienceId())
                .product(message.getProduct())
                .jobName(message.getJobName())
                .consumerRef(message.getUniqueConsumerRef())
                .timestamp(Instant.now().toString())
                .eventType(message.getEventType())
                .startTime(startTime)
                .endTime(startTime)
                .customerCount(
                        Optional.ofNullable(message.getBatchFiles())
                                .map(list -> list.stream().mapToLong(BatchFile::getCustomerCount).sum())
                                .orElse(0))
                .batchFiles(Optional.ofNullable(message.getBatchFiles()).orElse(Collections.emptyList())
                        .stream()
                        .map(f -> new ECPBatchAudit.BatchFileAudit(f.getBlobUrl(), f.getFilename(), f.getFileType()))
                        .collect(Collectors.toList()))
                .success(true)
                .retryFlag(false)
                .retryCount(0)
                .build();
        try {
            kafkaTemplate.send(auditTopic, objectMapper.writeValueAsString(audit));
            logger.info("📡 Published Fmcompose Audit for BatchId={}", message.getBatchId());
        } catch (Exception e) {
            logger.error("Error publishing Fmcompose audit: {}", e.getMessage(), e);
        }
    }

    private void sendFmCompleteAudit(KafkaMessage message, String summaryUrl, Instant startTime, Instant endTime,
                                    boolean success, boolean retryFlag, int retryCount) {
        ECPBatchAudit.BatchFileAudit fileAudit = new ECPBatchAudit.BatchFileAudit(summaryUrl, "summary.json", "JSON");
        ECPBatchAudit audit = ECPBatchAudit.builder()
                .datastreamName("Fmcomplete")
                .serviceName("Fmcomplete")
                .systemEnv(System.getenv().getOrDefault("SYSTEM_ENV", "DEV"))
                .batchId(message.getBatchId())
                .sourceSystem(message.getSourceSystem())
                .tenantCode(message.getTenantCode())
                .channelId(message.getChannelId())
                .audienceId(message.getAudienceId())
                .product(message.getProduct())
                .jobName(message.getJobName())
                .consumerRef(message.getUniqueConsumerRef())
                .timestamp(Instant.now().toString())
                .eventType(message.getEventType())
                .startTime(startTime)
                .endTime(endTime)
                .customerCount(
                        Optional.ofNullable(message.getBatchFiles())
                                .map(list -> list.stream().mapToLong(BatchFile::getCustomerCount).sum())
                                .orElse(0))
                .batchFiles(Collections.singletonList(fileAudit))
                .success(success)
                .retryFlag(retryFlag)
                .retryCount(retryCount)
                .build();
        try {
            kafkaTemplate.send(auditTopic, objectMapper.writeValueAsString(audit));
            logger.info("📡 Published Fmcomplete Audit for BatchId={}, Success={}", message.getBatchId(), success);
        } catch (Exception e) {
            logger.error("Error publishing Fmcomplete audit: {}", e.getMessage(), e);
        }
    }

    // Existing methods such as processAfterOT(), callOrchestrationBatchApi() etc. remain unchanged or need adaptation as per existing code
}



