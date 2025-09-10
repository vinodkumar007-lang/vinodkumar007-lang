package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.constants.AppConstants;
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
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;
import static com.nedbank.kafka.filemanage.constants.AppConstants.*;
import static com.nedbank.kafka.filemanage.utils.SummaryJsonWriter.extractAccountFromFileName;

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

    @Value("${ot.orchestration.api.url}")
    private String otOrchestrationApiUrl;

    @Value("${ot.service.mfc.url}")
    private String orchestrationMfcUrl;

    //@Value("${ot.auth.token}")
    //private String orchestrationAuthToken;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final BlobStorageService blobStorageService;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RestTemplate restTemplate = new RestTemplate();
    private final ExecutorService executor = Executors.newFixedThreadPool(5);

    @Autowired
    private SourceSystemProperties sourceSystemProperties;
    @Autowired
    public KafkaListenerService(BlobStorageService blobStorageService, KafkaTemplate<String, String> kafkaTemplate, SourceSystemProperties sourceSystemProperties) {
        this.blobStorageService = blobStorageService;
        this.kafkaTemplate = kafkaTemplate;
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
                    logger.error("❌ [batchId: {}] Failed to download or overwrite file: {} - {}", batchId, blobUrl, e.getMessage(), e);
                    throw e;
                }
            }

            // 🔁 New logic starts here — dynamic lookup
            String sourceSystem = message.getSourceSystem();
            Optional<SourceSystemProperties.SystemConfig> matchingConfig =
                    sourceSystemProperties.getConfigForSourceSystem(sourceSystem);

            if (matchingConfig.isEmpty()) {
                logger.error("❌ [batchId: {}] Unsupported or unconfigured source system '{}'", batchId, sourceSystem);
                ack.acknowledge();
                return;
            }

            SourceSystemProperties.SystemConfig config = matchingConfig.get();
            String url = config.getUrl();
            String token = blobStorageService.getOtdsToken();

            if (url == null || url.isBlank()) {
                logger.error("❌ [batchId: {}] Orchestration URL not configured for source system '{}'", batchId, sourceSystem);
                ack.acknowledge();
                return;
            }

            // ✅ Acknowledge before async OT call
            ack.acknowledge();

            String finalBatchId = batchId;
            executor.submit(() -> {
                try {
                    logger.info("🚀 [batchId: {}] Calling Orchestration API: {}", finalBatchId, url);
                    OTResponse otResponse = callOrchestrationBatchApi(token, url, message);
                    logger.info("📤 [batchId: {}] OT request sent successfully", finalBatchId);
                    processAfterOT(message, otResponse);
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

            Path jobDir = Paths.get(mountPath, AppConstants.OUTPUT_FOLDER, message.getSourceSystem(), otResponse.getJobId());
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

            String fileName = AppConstants.SUMMARY_FILENAME_PREFIX + batchId + AppConstants.JSON_EXTENSION;
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
            headers.set(AppConstants.HEADER_AUTHORIZATION, AppConstants.BEARER_PREFIX + token);
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<String> request = new HttpEntity<>(objectMapper.writeValueAsString(msg), headers);
            logger.debug("📨 OT Request Payload: {}", objectMapper.writeValueAsString(msg));

            ResponseEntity<Map> response = restTemplate.exchange(url, HttpMethod.POST, request, Map.class);
            logger.info("✅ Received OT response with status: {} for batchId: {}",
                    response.getStatusCode(), msg.getBatchId());

            List<Map<String, Object>> data = (List<Map<String, Object>>) response.getBody().get(AppConstants.OT_RESPONSE_DATA_KEY);
            if (data != null && !data.isEmpty()) {
                Map<String, Object> item = data.get(0);
                otResponse.setJobId((String) item.get(AppConstants.OT_JOB_ID_KEY));
                otResponse.setId((String) item.get(AppConstants.OT_ID_KEY));
                msg.setJobName(otResponse.getJobId());
                otResponse.setSuccess(true);

                logger.info("🎯 OT Job created successfully - JobID: {}, ID: {}, BatchID: {}",
                        otResponse.getJobId(), otResponse.getId(), msg.getBatchId());
            } else {
                logger.error("❌ No data found in OT orchestration response for batchId: {}", msg.getBatchId());
                otResponse.setSuccess(false);
                otResponse.setMessage(AppConstants.NO_OT_DATA_MESSAGE);
            }

            return otResponse;
        } catch (Exception e) {
            logger.error("❌ Exception during OT orchestration call for batchId: {} - {}",
                    msg.getBatchId(), e.getMessage(), e);
            otResponse.setSuccess(false);
            otResponse.setMessage(AppConstants.OT_CALL_FAILURE_PREFIX + e.getMessage());
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
        Path docgenRoot = Paths.get(mountPath, "jobs", jobId, id, AppConstants.DOCGEN_FOLDER);
        long startTime = System.currentTimeMillis();
        File xmlFile = null;

        while ((System.currentTimeMillis() - startTime) < rptMaxWaitSeconds * 1000L) {
            if (Files.exists(docgenRoot)) {
                try (Stream<Path> paths = Files.walk(docgenRoot)) {
                    Optional<Path> xmlPath = paths
                            .filter(Files::isRegularFile)
                            .filter(p -> p.getFileName().toString().equalsIgnoreCase(AppConstants.XML_FILE_NAME))
                            .findFirst();

                    if (xmlPath.isPresent()) {
                        xmlFile = xmlPath.get().toFile();

                        long size1 = xmlFile.length();
                        TimeUnit.SECONDS.sleep(1);
                        long size2 = xmlFile.length();

                        if (size1 > 0 && size1 == size2) {
                            logger.info(AppConstants.LOG_FOUND_STABLE_XML, xmlFile.getAbsolutePath());
                            return xmlFile;
                        } else {
                            logger.info(AppConstants.LOG_XML_SIZE_CHANGING, xmlFile.getAbsolutePath());
                        }
                    }
                } catch (IOException e) {
                    logger.warn(AppConstants.LOG_ERROR_SCANNING_FOLDER, jobId, id, e.getMessage(), e);
                }
            } else {
                logger.debug(AppConstants.LOG_DOCGEN_FOLDER_NOT_FOUND, jobId, id, docgenRoot);
            }

            TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
        }

        String errMsg = String.format(AppConstants.LOG_XML_TIMEOUT, docgenRoot, jobId, id);
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
                        .filter(path -> path.getFileName().toString().equalsIgnoreCase(AppConstants.ERROR_REPORT_FILE_NAME))
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
                            String account = parts[AppConstants.ERROR_REPORT_INDEX_ACCOUNT].trim();

                            String method = AppConstants.DEFAULT_METHOD;
                            if (parts.length >= 4) {
                                method = parts[AppConstants.ERROR_REPORT_INDEX_METHOD_V1].trim();
                            } else if (parts.length >= 3) {
                                method = parts[AppConstants.ERROR_REPORT_INDEX_METHOD_V2].trim();
                            }

                            String status = AppConstants.DEFAULT_STATUS;
                            if (parts.length >= 5) {
                                status = parts[AppConstants.ERROR_REPORT_INDEX_STATUS_V1].trim();
                            } else if (parts.length >= 4) {
                                status = parts[AppConstants.ERROR_REPORT_INDEX_STATUS_V2].trim();
                            }

                            if (method.isEmpty()) method = AppConstants.DEFAULT_METHOD;
                            if (status.isEmpty()) status = AppConstants.DEFAULT_STATUS;

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
// --- FIXED version: skip missing temp files safely ---
    private List<SummaryProcessedFile> buildDetailedProcessedFiles(
            Path jobDir,
            List<SummaryProcessedFile> customerList,
            Map<String, Map<String, String>> errorMap,
            KafkaMessage msg) throws IOException {

        List<SummaryProcessedFile> finalList = new ArrayList<>();
        if (jobDir == null || customerList == null || msg == null) return finalList;

        List<String> deliveryFolders = List.of(
                AppConstants.FOLDER_EMAIL,
                AppConstants.FOLDER_MOBSTAT,
                AppConstants.FOLDER_PRINT
        );

        Map<String, String> folderToOutputMethod = Map.of(
                AppConstants.FOLDER_EMAIL, AppConstants.OUTPUT_EMAIL,
                AppConstants.FOLDER_MOBSTAT, AppConstants.OUTPUT_MOBSTAT,
                AppConstants.FOLDER_PRINT, AppConstants.OUTPUT_PRINT
        );

        // -------- Upload all archive files and map by account + filename --------
        Path archivePath = jobDir.resolve(AppConstants.FOLDER_ARCHIVE);
        Map<String, Map<String, String>> accountToArchiveMap = new HashMap<>(); // account -> (filename -> URL)
        if (Files.exists(archivePath)) {
            try (Stream<Path> stream = Files.walk(archivePath)) {
                stream.filter(Files::isRegularFile).forEach(file -> {
                    // ✅ Skip temp/missing files
                    if (!Files.exists(file)) {
                        logger.warn("[{}] ⏩ Skipping missing archive file: {}", msg.getBatchId(), file);
                        return;
                    }

                    String fileName = file.getFileName().toString();
                    String account = extractAccountFromFileName(fileName);
                    if (account == null) return;

                    try {
                        String archiveUrl = decodeUrl(
                                blobStorageService.uploadFileByMessage(file.toFile(), AppConstants.FOLDER_ARCHIVE, msg)
                        );
                        accountToArchiveMap.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, archiveUrl);
                        logger.info("[{}] 📦 Uploaded archive file for account {}: {}", msg.getBatchId(), account, archiveUrl);
                    } catch (Exception e) {
                        logger.error("[{}] ⚠️ Failed to upload archive file {}: {}", msg.getBatchId(), fileName, e.getMessage(), e);
                    }
                });
            }
        }

        // -------- Upload delivery files --------
        Map<String, String> emailFileMap = new HashMap<>();
        Map<String, String> mobstatFileMap = new HashMap<>();
        Map<String, String> printFileMap = new HashMap<>();

        for (String folder : deliveryFolders) {
            Path folderPath = jobDir.resolve(folder);
            if (!Files.exists(folderPath)) continue;

            try (Stream<Path> stream = Files.walk(folderPath)) {
                stream.filter(Files::isRegularFile).forEach(file -> {
                    // ✅ Skip temp/missing files
                    if (!Files.exists(file)) {
                        logger.warn("[{}] ⏩ Skipping missing {} file: {}", msg.getBatchId(), folder, file);
                        return;
                    }

                    String fileName = file.getFileName().toString();
                    try {
                        String url = decodeUrl(
                                blobStorageService.uploadFileByMessage(file.toFile(), folder, msg)
                        );
                        switch (folder) {
                            case AppConstants.FOLDER_EMAIL -> emailFileMap.put(fileName, url);
                            case AppConstants.FOLDER_MOBSTAT -> mobstatFileMap.put(fileName, url);
                            case AppConstants.FOLDER_PRINT -> printFileMap.put(fileName, url);
                        }
                        logger.info("[{}] ✅ Uploaded {} file: {}", msg.getBatchId(), folderToOutputMethod.get(folder), url);
                    } catch (Exception e) {
                        logger.error("[{}] ⚠️ Failed to upload {} file {}: {}", msg.getBatchId(), folderToOutputMethod.get(folder), fileName, e.getMessage(), e);
                    }
                });
            }
        }

        // -------- Build final list --------
        Set<String> uniqueKeys = new HashSet<>();

        boolean isMfc = "MFC".equalsIgnoreCase(msg.getSourceSystem());

        for (SummaryProcessedFile customer : customerList) {
            if (customer == null || customer.getAccountNumber() == null) continue;

            String account = customer.getAccountNumber();
            Map<String, String> archivesForAccount = accountToArchiveMap.getOrDefault(account, Collections.emptyMap());

            for (Map.Entry<String, String> archiveEntry : archivesForAccount.entrySet()) {
                String archiveFileName = archiveEntry.getKey();
                String archiveUrl = archiveEntry.getValue();

                String key = customer.getCustomerId() + "|" + account + "|" + archiveFileName;
                if (uniqueKeys.contains(key)) continue;
                uniqueKeys.add(key);

                SummaryProcessedFile entry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, entry);
                entry.setArchiveBlobUrl(archiveUrl);

                if (isMfc) {
                    // 🔹 MFC: match delivery files by account
                    entry.setPdfEmailFileUrl(findFileByAccount(emailFileMap, account));
                    entry.setPdfMobstatFileUrl(findFileByAccount(mobstatFileMap, account));
                    entry.setPrintFileUrl(findFileByAccount(printFileMap, account));
                } else {
                    // 🔹 DEBTMAN: match delivery files by exact filename
                    entry.setPdfEmailFileUrl(emailFileMap.get(archiveFileName));
                    entry.setPdfMobstatFileUrl(mobstatFileMap.get(archiveFileName));
                    entry.setPrintFileUrl(printFileMap.get(archiveFileName));
                }

                finalList.add(entry);
            }
        }

        logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
        return finalList;
    }

    // --- Helper for MFC to match by account ---
    private String findFileByAccount(Map<String, String> fileMap, String account) {
        if (account == null) return null;
        return fileMap.entrySet().stream()
                .filter(e -> {
                    String fileName = e.getKey();
                    return fileName.startsWith(account + "_")               // case: 12345_statement.pdf
                            || fileName.contains("_" + account + "_")      // case: statement_12345_extra.pdf
                            || fileName.endsWith("_" + account + ".pdf");  // case: Statement-2025-08-02_12345.pdf (MFC)
                })
                .map(Map.Entry::getValue)
                .findFirst()
                .orElse(null);
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

        Path printDir = jobDir.resolve(AppConstants.PRINT_FOLDER_NAME);
        if (!Files.exists(printDir)) {
            logger.info("ℹ️ No '{}' directory found in jobDir: {}", AppConstants.PRINT_FOLDER_NAME, jobDir);
            return printFiles;
        }

        try (Stream<Path> stream = Files.list(printDir)) {
            stream.filter(Files::isRegularFile).forEach(f -> {
                try {
                    String fileName = f.getFileName() != null ? f.getFileName().toString() : AppConstants.UNKNOWN_FILE_NAME;
                    String uploadPath = msg.getSourceSystem() + "/" +  msg.getBatchId() + "/" + msg.getUniqueConsumerRef() + "/" +AppConstants.PRINT_FOLDER_NAME + "/" + fileName;

                    String blob = blobStorageService.uploadFile(f.toFile(), uploadPath);
                    printFiles.add(new PrintFile(blob));

                    logger.info("📤 Uploaded print file: {} -> {}", fileName, blob);
                } catch (Exception e) {
                    logger.warn("⚠️ Failed to upload print file: {}", f, e);
                }
            });
        } catch (IOException e) {
            logger.error("❌ Failed to list files in '{}' directory: {}", AppConstants.PRINT_FOLDER_NAME, printDir, e);
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
                String customersProcessed = outputList.getAttribute(AppConstants.CUSTOMERS_PROCESSED_KEY);
                String pagesProcessed = outputList.getAttribute(AppConstants.PAGES_PROCESSED_KEY);

                int custCount = (customersProcessed != null && !customersProcessed.isBlank())
                        ? Integer.parseInt(customersProcessed.trim()) : 0;
                int pageCount = (pagesProcessed != null && !pagesProcessed.isBlank())
                        ? Integer.parseInt(pagesProcessed.trim()) : 0;

                summaryCounts.put(AppConstants.CUSTOMERS_PROCESSED_KEY, custCount);
                summaryCounts.put(AppConstants.PAGES_PROCESSED_KEY, pageCount);

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
