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
        try {
            KafkaMessage message = objectMapper.readValue(rawMessage, KafkaMessage.class);

            List<BatchFile> dataFiles = message.getBatchFiles().stream()
                    .filter(f -> "DATA".equalsIgnoreCase(f.getFileType()))
                    .toList();
            message.setBatchFiles(dataFiles);

            String batchId = message.getBatchId();
            Path batchDir = Paths.get(mountPath, "input", message.getSourceSystem(), batchId);
            Files.createDirectories(batchDir);

            for (BatchFile file : dataFiles) {
                String blobUrl = file.getBlobUrl();
                String content = blobStorageService.downloadFileContent(blobUrl);
                Path localPath = batchDir.resolve(message.getSourceSystem() + ".csv");
                Files.write(localPath, content.getBytes(StandardCharsets.UTF_8));
                file.setBlobUrl(localPath.toString());
            }

            writeAndUploadMetadataJson(message, batchDir);

            OTResponse otResponse = callOrchestrationBatchApi("eyJraWQiOiJjZjkwMjJmMjUxNjM2MjQzNjI5YmE1ZmNmMjMwZDI4YzFlOTJkNDNiIiwidHlwIjoiSldUIiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiIxZGY1MmRlMy1hYTJhLTQwMDUtODBmMi1jYzljMTY5NDU4ZDAiLCJzY3AiOlsib3Rkczpncm91cHMiLCJvdGRzOnJvbGVzIl0sInJvbGUiOltdLCJncnAiOlsidGVuYW50YWRtaW5zQGV4c3RyZWFtLnJvbGUiLCJvdGRzYWRtaW5zQG90ZHMuYWRtaW4iLCJvdGFkbWluc0BvdGRzLmFkbWluIiwiZW1wb3dlcmFkbWluc0BleHN0cmVhbS5yb2xlIl0sImRtcCI6eyJPVERTX0NSRURTX0FVVEgiOiJ0cnVlIiwiT1REU19IQVNfUEFTU1dPUkQiOiJmYWxzZSJ9LCJydGkiOiI1ZjFkMzFjNC02ZTdkLTRlYWEtOTU3MC1hMGY4OWJiOGI3NTUiLCJzYXQiOjE3NTIyNDU2NTcsImlzcyI6Imh0dHBzOi8vZGV2LWV4c3RyZWFtLm5lZG5ldC5jby56YTo0NDMvb3Rkcy9vdGRzd3MiLCJncnQiOiJwYXNzd29yZCIsInN1Yl90eXAiOjAsInR5cCI6ImFjY2Vzc190b2tlbiIsInBpZCI6ImV4c3RyZWFtLnJvbGUiLCJyaWQiOnt9LCJ0aWQiOiJkZXYtZXhzdHJlYW0iLCJzaWQiOiIxZmQ2YmI4NC00YjY0LTQzZDgtOTJiMS1kY2U2YWIzZDQ3OWYiLCJ1aWQiOiJ0ZW5hbnRhZG1pbkBleHN0cmVhbS5yb2xlIiwidW5tIjoidGVuYW50YWRtaW4iLCJuYW1lIjoidGVuYW50YWRtaW4iLCJleHAiOjE3ODM3ODE2NTcsImlhdCI6MTc1MjI0NTY1NywianRpIjoiMGU4ZWI4NzYtOWJmYi00OTczLWFiN2ItM2EyZTg4NWM5N2MzIiwiY2lkIjoiZGV2ZXhzdHJlYW1jbGllbnQifQ.JdXQ7pDNlEBS8jOny0yhKrC85CsypDdJzjww_OhVKL4BNBLQRfJf04ESqcnoONEIfbeARLGPS6THMP6K6xOeHcO7oViTFtgXg27jhrfj6OXiU52pAvo2qFBAs6VvTueNjDOyQMsau-PzigYdPNw86IWzeK0Ude7DhaR1rNTPbu7LsqKHM3aD6SFli0EeLSux5eJYdWqTy2gpH4iNodxPjlyt5i6UoNEwl1TqUwbMEtbztfrGiwMPXvSflGBH10pSDDtNpssiyvsDl_flnqLmqxso-Ff5AVs8eAjHgsQnSEIeQQp9sX0JoSbNgW8D0iACdlI-6f9onOLg4JW-Ozucmg", message);
            if (otResponse == null) {
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
            logger.info("🟡 OT request sent, response sent to output topic with status=PENDING");

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

            DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
            DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
            Document doc = dBuilder.parse(xmlFile);
            doc.getDocumentElement().normalize();

            Map<String, String> accountCustomerMap = extractAccountCustomerMapFromDoc(doc);
            Path jobDir = Paths.get(mountPath, "output", message.getSourceSystem(), otResponse.getJobId());

            List<SummaryProcessedFile> processedFiles = buildAndUploadProcessedFiles(jobDir, accountCustomerMap, message);
           
            // Add error report failures (if any)
            String errorReportPath = Paths.get(jobDir.toString(), "ErrorReport.csv").toString();
            List<SummaryProcessedFile> failures = appendFailureEntries(errorReportPath, accountCustomerMap);
            processedFiles.addAll(failures);

            List<PrintFile> printFiles = uploadPrintFiles(jobDir, message);

            String triggerPath = jobDir.resolve("mobstat_trigger/DropData.trigger").toString();
            if (Files.exists(Paths.get(triggerPath))) {
                blobStorageService.uploadFile(new File(triggerPath), message.getSourceSystem() + "/mobstat_trigger/DropData.trigger");
            }

            SummaryPayload payload = SummaryJsonWriter.buildPayload(
                    message, processedFiles, printFiles, triggerPath, 0);
            String summaryPath = SummaryJsonWriter.writeSummaryJsonToFile(payload);
            String summaryUrl = blobStorageService.uploadSummaryJson(summaryPath, message, "summary_" + message.getBatchId() + ".json");
            payload.setSummaryFileURL(decodeUrl(summaryUrl));

            logger.info("📄 Summary JSON:");
            logger.info(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(payload));

            SummaryResponse response = new SummaryResponse();
            response.setBatchID(message.getBatchId());
            response.setFileName(payload.getFileName());
            response.setHeader(payload.getHeader());
            response.setMetadata(payload.getMetadata());
            response.setPayload(payload.getPayload());
            response.setSummaryFileURL(payload.getSummaryFileURL());

            ApiResponse finalResponse = new ApiResponse("Summary generated", "COMPLETED", response);

            logger.info("📤 Final API Response:");
            logger.info(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(finalResponse));

            kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(finalResponse));
            logger.info("✅ Final summary sent to Kafka output topic");
        } catch (Exception e) {
            logger.error("❌ Error post-OT summary generation", e);
        }
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
        logger.info("🔍 Looking for _STDDELIVERYFILE.xml under {}", docgenRoot);

        long startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < rptMaxWaitSeconds * 1000L) {
            if (!Files.exists(docgenRoot)) {
                logger.info("📂 docgen folder not yet available. Retrying in {}ms...", rptPollIntervalMillis);
                TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
                continue;
            }

            try (Stream<Path> paths = Files.walk(docgenRoot)) {
                Optional<Path> xmlPath = paths
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().equalsIgnoreCase("_STDDELIVERYFILE.xml"))
                        .findFirst();

                if (xmlPath.isPresent()) {
                    File xmlFile = xmlPath.get().toFile();

                    if (xmlFile.length() == 0) {
                        logger.info("⏳ XML file found but still empty. Waiting...");
                        TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
                        continue;
                    }

                    try {
                        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
                        dBuilder.parse(xmlFile);
                        logger.info("✅ Valid and complete XML file found: {}", xmlFile.getAbsolutePath());
                        return xmlFile;
                    } catch (Exception e) {
                        logger.info("⏳ XML file found but still being written (not parseable). Waiting...");
                        TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
                    }
                }
            } catch (IOException e) {
                logger.warn("⚠️ Error while scanning docgen folder", e);
            }

            TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
        }

        logger.error("❌ Timed out after {} seconds waiting for complete _STDDELIVERYFILE.xml", rptMaxWaitSeconds);
        return null;
    }

    private List<SummaryProcessedFile> appendFailureEntries(String errorReportFilePath, Map<String, String> successMap) {
    List<SummaryProcessedFile> failures = new ArrayList<>();
    if (errorReportFilePath == null) return failures;

    Path path = Paths.get(errorReportFilePath);
    if (!Files.exists(path)) return failures;

    try (BufferedReader reader = Files.newBufferedReader(path)) {
        String line;
        while ((line = reader.readLine()) != null) {
            String[] parts = line.split("\\|");
            if (parts.length >= 5) {
                String account = parts[0].trim();
                String customer = parts[1].trim();
                String outputMethod = parts[2].trim().toUpperCase();
                String status = parts[4].trim();

                if (!successMap.containsKey(account) && "FAILED".equalsIgnoreCase(status)) {
                    SummaryProcessedFile failEntry = new SummaryProcessedFile();
                    failEntry.setAccountNumber(account);
                    failEntry.setCustomerId(customer);
                    failEntry.setStatusCode("FAILURE");
                    failEntry.setStatusDescription("Processing failed");

                    // Set FAILED URL for the appropriate delivery method
                    switch (outputMethod) {
                        case "EMAIL"   -> failEntry.setPdfEmailFileUrl("FAILED");
                        case "HTML"    -> failEntry.setHtmlEmailFileUrl("FAILED");
                        case "MOBSTAT" -> failEntry.setPdfMobstatFileUrl("FAILED");
                        case "TXT"     -> failEntry.setTxtEmailFileUrl("FAILED");
                        case "ARCHIVE" -> failEntry.setPdfArchiveFileUrl("FAILED");
                        default -> logger.warn("⚠️ Unknown OutputMethod in ErrorReport: {}", outputMethod);
                    }

                    failures.add(failEntry);
                }
            }
        }
    } catch (IOException e) {
        logger.error("❌ Failed to read error report file", e);
    }

    logger.info("📉 Appended {} failure entries from ErrorReport", failures.size());
    return failures;
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
                    logger.warn("Print upload failed", e);
                }
            });
        } catch (IOException ignored) {}
        return printFiles;
    }

    private List<SummaryProcessedFile> buildAndUploadProcessedFiles(Path jobDir, Map<String, String> accountMap, KafkaMessage msg) throws IOException {
        List<SummaryProcessedFile> list = new ArrayList<>();
        for (String folder : List.of("archive", "email", "html", "mobstat", "txt")) {
            Path subDir = jobDir.resolve(folder);
            if (!Files.exists(subDir)) continue;
            Files.list(subDir).filter(Files::isRegularFile).forEach(file -> {
                try {
                    String fileName = file.getFileName().toString();
                    String account = extractAccountFromFileName(fileName);
                    String customer = accountMap.get(account);
                    if (account == null || customer == null) return;

                    SummaryProcessedFile entry = new SummaryProcessedFile();
                    entry.setAccountNumber(account);
                    entry.setCustomerId(customer);
                    entry.setStatusCode("OK");
                    entry.setStatusDescription("Success");

                    String blob = blobStorageService.uploadFile(file.toFile(), String.format("%s/%s/%s/%s",
                            msg.getSourceSystem(), msg.getBatchId(), folder, fileName));
                    String decoded = decodeUrl(blob);

                    switch (folder) {
                        case "archive" -> entry.setPdfArchiveFileUrl(decoded);
                        case "email" -> entry.setPdfEmailFileUrl(decoded);
                        case "html" -> entry.setHtmlEmailFileUrl(decoded);
                        case "mobstat" -> entry.setPdfMobstatFileUrl(decoded);
                        case "txt" -> entry.setTxtEmailFileUrl(decoded);
                    }
                    list.add(entry);
                } catch (Exception ignored) {}
            });
        }
        return list;
    }

    private String extractAccountFromFileName(String name) {
        Matcher m = Pattern.compile("(\\d{9,})").matcher(name);
        return m.find() ? m.group(1) : null;
    }

    private String decodeUrl(String url) {
        try {
            return URLDecoder.decode(url, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return url;
        }
    }

    private OTResponse callOrchestrationBatchApi(String token, KafkaMessage msg) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.set("Authorization", "Bearer " + token);
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<String> request = new HttpEntity<>(objectMapper.writeValueAsString(msg), headers);
            ResponseEntity<Map> response = restTemplate.exchange(otOrchestrationApiUrl, HttpMethod.POST, request, Map.class);

            logger.info("📨 OT Orchestration Response {}" , objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(response.getBody()));

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
