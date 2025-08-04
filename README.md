2025-08-04 12:32:25.202+0000 [32m- INFO[0;39m --- [     pool-4-thread-6] [a682f8b989692ec8,9aaff39e5a92585d,,] c.o.e.c.c.m.DistributionProducer         : Seeing message 'A user canceled the engine run.' for canceled flow step 3fb95e56-ffad-4ffc-a0ab-938585537697
2025-08-04 12:32:25.202+0000 [32m- INFO[0;39m --- [     pool-4-thre

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
import java.util.*;
import java.util.concurrent.*;
import java.util.stream.*;
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
                    .filter(f -> "DATA".equalsIgnoreCase(f.getFileType()))
                    .count();
            long refCount = batchFiles.stream()
                    .filter(f -> "REF".equalsIgnoreCase(f.getFileType()))
                    .count();

            // 1. DATA only ✅
            if (dataCount == 1 && refCount == 0) {
                logger.info("✅ [batchId: {}] Valid with 1 DATA file", batchId);
            }
            // 2. Multiple DATA ❌
            else if (dataCount > 1) {
                logger.error("❌ [batchId: {}] Rejected - Multiple DATA files", batchId);
                ack.acknowledge();
                return;
            }
            // 3. REF only ❌
            else if (dataCount == 0 && refCount > 0) {
                logger.error("❌ [batchId: {}] Rejected - Only REF files", batchId);
                ack.acknowledge();
                return;
            }
            // ✅ 4. DATA + REF — pass both to OT
            else if (dataCount == 1 && refCount > 0) {
                logger.info("✅ [batchId: {}] Valid with DATA + REF files (both will be passed to OT)", batchId);
                message.setBatchFiles(batchFiles);
            }
            // 5. Unknown or empty file types ❌
            else {
                logger.error("❌ [batchId: {}] Rejected - Invalid or unsupported file type combination", batchId);
                ack.acknowledge();
                return;
            }

            String sanitizedBatchId = batchId.replaceAll("[^a-zA-Z0-9_-]", "_");
            String sanitizedSourceSystem = message.getSourceSystem().replaceAll("[^a-zA-Z0-9_-]", "_");

            Path batchDir = Paths.get(mountPath, "input", sanitizedSourceSystem, sanitizedBatchId);
            if (Files.exists(batchDir)) {
                logger.warn("⚠️ [batchId: {}] Directory already exists at path: {}", batchId, batchDir);
            } else {
                Files.createDirectories(batchDir);
                logger.info("📁 [batchId: {}] Created input directory: {}", batchId, batchDir);
            }

            for (BatchFile file : message.getBatchFiles()) {
                String blobUrl = file.getBlobUrl();
                Path localPath = batchDir.resolve(file.getFilename());
                blobStorageService.downloadFileToLocal(blobUrl, localPath);
                file.setBlobUrl(localPath.toString());
                logger.info("⬇️ [batchId: {}] Downloaded file: {} to {}", batchId, blobUrl, localPath);
            }

            String sourceSystem = message.getSourceSystem();

            if ("DEBTMAN".equalsIgnoreCase(sourceSystem) && (otOrchestrationApiUrl == null || otOrchestrationApiUrl.isBlank())) {
                logger.error("❌ [batchId: {}] otOrchestrationApiUrl is not configured for 'DEBTMAN'", batchId);
                throw new IllegalArgumentException("otOrchestrationApiUrl is not configured");
            }

            if ("MFC".equalsIgnoreCase(sourceSystem) && (orchestrationMfcUrl == null || orchestrationMfcUrl.isBlank())) {
                logger.error("❌ [batchId: {}] orchestrationMfcUrl is not configured for 'MFC'", batchId);
                throw new IllegalArgumentException("orchestrationMfcUrl is not configured");
            }

            String url = switch (sourceSystem.toUpperCase()) {
                case "DEBTMAN" -> otOrchestrationApiUrl;
                case "MFC" -> orchestrationMfcUrl;
                default -> {
                    logger.error("❌ [batchId: {}] Unsupported source system '{}'", batchId, sourceSystem);
                    throw new IllegalArgumentException("Unsupported source system: " + sourceSystem);
                }
            };

            if (url == null || url.isBlank()) {
                logger.error("❌ [batchId: {}] Orchestration URL not configured for source system '{}'", batchId, sourceSystem);
                ack.acknowledge();
                return;
            }

            logger.info("🚀 [batchId: {}] Calling Orchestration API: {}", batchId, url);
            OTResponse otResponse = callOrchestrationBatchApi(orchestrationAuthToken, url, message);

            logger.info("📤 [batchId: {}] OT request sent successfully", batchId);
            ack.acknowledge();

            executor.submit(() -> processAfterOT(message, otResponse));

        } catch (Exception ex) {
            logger.error("❌ [batchId: {}] Kafka message processing failed. Error: {}", batchId, ex.getMessage(), ex);
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

                    Map<String, String> deliveryStatus = errorMap.getOrDefault(acc, new HashMap<>());
                    cs.setDeliveryStatus(deliveryStatus);

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
            logger.error("❌ Failed parsing STD XML file: {}", xmlFile.getAbsolutePath(), e);
            throw new RuntimeException("Failed to parse XML file: " + xmlFile.getName(), e);
        }
        return list;
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

            Path jobDir = Paths.get(mountPath, "output", message.getSourceSystem(), otResponse.getJobId());

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

            String fileName = "summary_" + batchId + ".json";
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
        try (Stream<Path> stream = Files.list(jobDir)) {
            Optional<Path> trigger = stream.filter(Files::isRegularFile)
                    .filter(p -> p.getFileName().toString().toLowerCase().endsWith(".trigger"))
                    .findFirst();

            if (trigger.isPresent()) {
                Path triggerFile = trigger.get();
                String blobUrl = blobStorageService.uploadFile(triggerFile.toFile(),
                        message.getSourceSystem() + "/" + message.getBatchId() + "/" + triggerFile.getFileName());

                logger.info("📤 Uploaded MOBSTAT trigger file: {} -> {}", triggerFile, blobUrl);
                return decodeUrl(blobUrl);
            } else {
                logger.warn("⚠️ No .trigger file found in MOBSTAT job directory: {}", jobDir);
                return null;
            }

        } catch (IOException e) {
            logger.error("⚠️ Failed to scan for .trigger file in jobDir: {}", jobDir, e);
            return null; // ✅ changed to avoid stopping program
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
            headers.set("Authorization", "Bearer " + token);
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<String> request = new HttpEntity<>(objectMapper.writeValueAsString(msg), headers);
            logger.debug("📨 OT Request Payload: {}", objectMapper.writeValueAsString(msg));

            ResponseEntity<Map> response = restTemplate.exchange(url, HttpMethod.POST, request, Map.class);
            logger.info("✅ Received OT response with status: {} for batchId: {}",
                    response.getStatusCode(), msg.getBatchId());

            List<Map<String, Object>> data = (List<Map<String, Object>>) response.getBody().get("data");
            if (data != null && !data.isEmpty()) {
                Map<String, Object> item = data.get(0);
                otResponse.setJobId((String) item.get("jobId"));
                otResponse.setId((String) item.get("id"));
                msg.setJobName(otResponse.getJobId());
                otResponse.setSuccess(true);

                logger.info("🎯 OT Job created successfully - JobID: {}, ID: {}, BatchID: {}",
                        otResponse.getJobId(), otResponse.getId(), msg.getBatchId());
            } else {
                logger.error("❌ No data found in OT orchestration response for batchId: {}", msg.getBatchId());
                otResponse.setSuccess(false);
                otResponse.setMessage("No data in OT orchestration response");
            }

            return otResponse;
        } catch (Exception e) {
            logger.error("❌ Exception during OT orchestration call for batchId: {} - {}",
                    msg.getBatchId(), e.getMessage(), e);
            otResponse.setSuccess(false);
            otResponse.setMessage("Failed OT orchestration call: " + e.getMessage());
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
                    logger.warn("⚠️ Error scanning docgen folder for jobId={} id={}: {}", jobId, id, e.getMessage(), e);
                }
            } else {
                logger.debug("🔍 docgen folder not found yet for jobId={}, id={}: {}", jobId, id, docgenRoot);
            }

            TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
        }

        String errMsg = String.format("❌ Timeout while waiting for stable XML file (_STDDELIVERYFILE.xml) in path: %s for jobId=%s, id=%s", docgenRoot, jobId, id);
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
            String jobRootPath = mountPath+"/jobs"; //"/mnt/nfs/dev-exstream/dev-SA/jobs";
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
                        .filter(path -> path.getFileName().toString().equalsIgnoreCase("ErrorReport.csv"))
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
                            String account = parts[0].trim();
                            String method = (parts.length >= 4 ? parts[3] : parts[2]).trim().toUpperCase();
                            String status = (parts.length >= 5 ? parts[4] : (parts.length >= 4 ? parts[3] : "Failed")).trim();

                            if (method.isEmpty()) method = "UNKNOWN";
                            if (status.isEmpty()) status = "Failed";

                            result.computeIfAbsent(account, k -> new HashMap<>()).put(method, status);
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
        List<String> deliveryFolders = List.of("email", "mobstat", "print");
        Map<String, String> folderToOutputMethod = Map.of(
                "email", "EMAIL",
                "mobstat", "MOBSTAT",
                "print", "PRINT"
        );

        if (jobDir == null || customerList == null || msg == null) {
            logger.warn("⚠️ One or more input parameters are null: jobDir={}, customerList={}, msg={}",
                    jobDir, customerList, msg);
            return finalList;
        }

        Path archivePath = jobDir.resolve("archive");

        for (SummaryProcessedFile customer : customerList) {
            if (customer == null) continue;

            String account = customer.getAccountNumber();
            if (account == null || account.isBlank()) {
                logger.warn("⚠️ Skipping customer with empty account number");
                continue;
            }

            // Archive upload
            String archiveBlobUrl = null;
            try {
                if (Files.exists(archivePath)) {
                    Optional<Path> archiveFile = Files.list(archivePath)
                            .filter(Files::isRegularFile)
                            .filter(p -> p.getFileName().toString().contains(account))
                            .findFirst();

                    if (archiveFile.isPresent()) {
                        archiveBlobUrl = blobStorageService.uploadFileByMessage(
                                archiveFile.get().toFile(), "archive", msg);

                        SummaryProcessedFile archiveEntry = new SummaryProcessedFile();
                        BeanUtils.copyProperties(customer, archiveEntry);
                        archiveEntry.setOutputType("ARCHIVE");
                        archiveEntry.setBlobUrl(decodeUrl(archiveBlobUrl));

                        finalList.add(archiveEntry);
                        logger.info("📦 Uploaded archive file for account {}: {}", account, archiveBlobUrl);
                    }
                }
            } catch (Exception e) {
                logger.warn("⚠️ Failed to upload archive file for account {}: {}", account, e.getMessage(), e);
            }

            // EMAIL, MOBSTAT, PRINT uploads
            for (String folder : deliveryFolders) {
                String outputMethod = folderToOutputMethod.get(folder);
                Path methodPath = jobDir.resolve(folder);
                String blobUrl = null;

                try {
                    if (Files.exists(methodPath)) {
                        Optional<Path> match = Files.list(methodPath)
                                .filter(Files::isRegularFile)
                                .filter(p -> p.getFileName().toString().contains(account))
                                .findFirst();

                        if (match.isPresent()) {
                            blobUrl = blobStorageService.uploadFileByMessage(match.get().toFile(), folder, msg);
                            logger.info("✅ Uploaded {} file for account {}: {}", outputMethod, account, blobUrl);
                        }
                    }
                } catch (Exception e) {
                    logger.warn("⚠️ Failed to upload {} file for account {}: {}", outputMethod, account, e.getMessage(), e);
                }

                SummaryProcessedFile entry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, entry);
                entry.setOutputType(outputMethod);
                entry.setBlobUrl(decodeUrl(blobUrl));

                if (archiveBlobUrl != null) {
                    entry.setArchiveOutputType("ARCHIVE");
                    entry.setArchiveBlobUrl(archiveBlobUrl);
                }

                finalList.add(entry);
            }
        }

        return finalList;
    }
    /**
     * Utility method to check if a string contains only digits.
     *
     * @param str Input string
     * @return true if numeric, false otherwise
     */
    // Helper methods
    private boolean isNumeric(String str) {
        return str != null && str.matches("\\d+");
    }
    /**
     * Creates a deep copy of SummaryProcessedFile using BeanUtils.
     *
     * @param original Original SummaryProcessedFile
     * @return Copied instance
     */
    private SummaryProcessedFile buildCopy(SummaryProcessedFile original) {
        SummaryProcessedFile copy = new SummaryProcessedFile();
        BeanUtils.copyProperties(original, copy);
        return copy;
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

        Path printDir = jobDir.resolve("print");
        if (!Files.exists(printDir)) {
            logger.info("ℹ️ No 'print' directory found in jobDir: {}", jobDir);
            return printFiles;
        }

        try (Stream<Path> stream = Files.list(printDir)) {
            stream.filter(Files::isRegularFile).forEach(f -> {
                try {
                    String fileName = f.getFileName() != null ? f.getFileName().toString() : "unknown_file";
                    String uploadPath = msg.getSourceSystem() + "/print/" + fileName;

                    String blob = blobStorageService.uploadFile(f.toFile(), uploadPath);
                    printFiles.add(new PrintFile(blob));

                    logger.info("📤 Uploaded print file: {} -> {}", fileName, blob);
                } catch (Exception e) {
                    logger.warn("⚠️ Failed to upload print file: {}", f, e);
                }
            });
        } catch (IOException e) {
            logger.error("❌ Failed to list files in print directory: {}", printDir, e);
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
                String customersProcessed = outputList.getAttribute("customersProcessed");
                String pagesProcessed = outputList.getAttribute("pagesProcessed");

                int custCount = (customersProcessed != null && !customersProcessed.isBlank())
                        ? Integer.parseInt(customersProcessed.trim()) : 0;
                int pageCount = (pagesProcessed != null && !pagesProcessed.isBlank())
                        ? Integer.parseInt(pagesProcessed.trim()) : 0;

                summaryCounts.put("customersProcessed", custCount);
                summaryCounts.put("pagesProcessed", pageCount);

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
