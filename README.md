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
import org.springframework.web.client.RestTemplate;
import org.w3c.dom.*;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import java.io.*;
import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
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
            KafkaMessage kafkaMessage = objectMapper.readValue(message, KafkaMessage.class);
            ApiResponse response = processSingleMessage(kafkaMessage);
            kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(response));
        } catch (Exception ex) {
            logger.error("❌ Error processing Kafka message", ex);
        }
    }

    public ApiResponse processSingleMessage(KafkaMessage message) {
    try {
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

        // ✅ Use your actual token
        String token = "eyJraWQiOiJjZjkwMjJmMjUxNjM2MjQzNjI5YmE1ZmNmMjMwZDI4YzFlOTJkNDNiIiwidHlwIjoiSldUIiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiIxZGY1MmRlMy1hYTJhLTQwMDUtODBmMi1jYzljMTY5NDU4ZDAiLCJzY3AiOlsib3Rkczpncm91cHMiLCJvdGRzOnJvbGVzIl0sInJvbGUiOltdLCJncnAiOlsidGVuYW50YWRtaW5zQGV4c3RyZWFtLnJvbGUiLCJvdGRzYWRtaW5zQG90ZHMuYWRtaW4iLCJvdGFkbWluc0BvdGRzLmFkbWluIiwiZW1wb3dlcmFkbWluc0BleHN0cmVhbS5yb2xlIl0sImRtcCI6eyJPVERTX0NSRURTX0FVVEgiOiJ0cnVlIiwiT1REU19IQVNfUEFTU1dPUkQiOiJmYWxzZSJ9LCJydGkiOiI1ZjFkMzFjNC02ZTdkLTRlYWEtOTU3MC1hMGY4OWJiOGI3NTUiLCJzYXQiOjE3NTIyNDU2NTcsImlzcyI6Imh0dHBzOi8vZGV2LWV4c3RyZWFtLm5lZG5ldC5jby56YTo0NDMvb3Rkcy9vdGRzd3MiLCJncnQiOiJwYXNzd29yZCIsInN1Yl90eXAiOjAsInR5cCI6ImFjY2Vzc190b2tlbiIsInBpZCI6ImV4c3RyZWFtLnJvbGUiLCJyaWQiOnt9LCJ0aWQiOiJkZXYtZXhzdHJlYW0iLCJzaWQiOiIxZmQ2YmI4NC00YjY0LTQzZDgtOTJiMS1kY2U2YWIzZDQ3OWYiLCJ1aWQiOiJ0ZW5hbnRhZG1pbkBleHN0cmVhbS5yb2xlIiwidW5tIjoidGVuYW50YWRtaW4iLCJuYW1lIjoidGVuYW50YWRtaW4iLCJleHAiOjE3ODM3ODE2NTcsImlhdCI6MTc1MjI0NTY1NywianRpIjoiMGU4ZWI4NzYtOWJmYi00OTczLWFiN2ItM2EyZTg4NWM5N2MzIiwiY2lkIjoiZGV2ZXhzdHJlYW1jbGllbnQifQ.JdXQ7pDNlEBS8jOny0yhKrC85CsypDdJzjww_OhVKL4BNBLQRfJf04ESqcnoONEIfbeARLGPS6THMP6K6xOeHcO7oViTFtgXg27jhrfj6OXiU52pAvo2qFBAs6VvTueNjDOyQMsau-PzigYdPNw86IWzeK0Ude7DhaR1rNTPbu7LsqKHM3aD6SFli0EeLSux5eJYdWqTy2gpH4iNodxPjlyt5i6UoNEwl1TqUwbMEtbztfrGiwMPXvSflGBH10pSDDtNpssiyvsDl_flnqLmqxso-Ff5AVs8eAjHgsQnSEIeQQp9sX0JoSbNgW8D0iACdlI-6f9onOLg4JW-Ozucmg";
        OTResponse otResponse = callOrchestrationBatchApi(token, message);
        if (otResponse == null) return new ApiResponse("OT call failed", "error", null);

        File xmlFile = waitForXmlFile(otResponse.getJobId(), otResponse.getId());
        if (xmlFile == null) return new ApiResponse("_STDDELIVERYFILE.xml not found", "error", null);

        DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
        DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
        Document doc = dBuilder.parse(xmlFile);
        doc.getDocumentElement().normalize();

        int customersProcessed = 0;
        NodeList outputListNodes = doc.getElementsByTagName("outputList");
        if (outputListNodes.getLength() > 0) {
            Element outputList = (Element) outputListNodes.item(0);
            String val = outputList.getAttribute("customersProcessed");
            if (val != null) {
                try {
                    customersProcessed = Integer.parseInt(val);
                } catch (NumberFormatException ignored) {}
            }
        }

        String errorReportFilePath = null;
        NodeList reportFileNodes = doc.getElementsByTagName("reportFile");
        for (int i = 0; i < reportFileNodes.getLength(); i++) {
            Element reportFile = (Element) reportFileNodes.item(i);
            if ("Error_Report".equalsIgnoreCase(reportFile.getAttribute("dataFile"))) {
                errorReportFilePath = reportFile.getAttribute("name");
                break;
            }
        }

        Map<String, String> accountCustomerMap = extractAccountCustomerMapFromDoc(doc);
        Path jobDir = Paths.get(mountPath, "output", message.getSourceSystem(), otResponse.getJobId());
        List<SummaryProcessedFile> processedFiles = buildAndUploadProcessedFiles(jobDir, accountCustomerMap, message);

        Map<String, String> successMap = new HashMap<>();
        for (SummaryProcessedFile s : processedFiles) successMap.put(s.getAccountNumber(), s.getCustomerId());
        processedFiles.addAll(appendFailureEntries(errorReportFilePath, successMap));

        List<PrintFile> printFiles = uploadPrintFiles(jobDir, message);
        String mobstatTriggerPath = jobDir.resolve("mobstat_trigger/DropData.trigger").toString();

        // ✅ Upload DropData.trigger if exists
        Path triggerFile = Paths.get(mobstatTriggerPath);
        if (Files.exists(triggerFile)) {
            String remotePath = String.format("%s/%s/%s/mobstat_trigger/DropData.trigger",
                    message.getSourceSystem(), message.getBatchId(), message.getUniqueConsumerRef());
            blobStorageService.uploadFile(triggerFile.toFile(), remotePath);
        }

        // ✅ Build full summary.json (with full details)
        SummaryPayload payload = SummaryJsonWriter.buildPayload(
                message, processedFiles, printFiles, mobstatTriggerPath, customersProcessed);

        String summaryPath = SummaryJsonWriter.writeSummaryJsonToFile(payload);
        String summaryUrl = blobStorageService.uploadSummaryJson(summaryPath, message, "summary_" + batchId + ".json");
        payload.setSummaryFileURL(decodeUrl(summaryUrl));

        // ✅ Send only light final response — no print/processed files
        SummaryResponse summaryResponse = new SummaryResponse();
        summaryResponse.setBatchID(batchId);
        summaryResponse.setFileName(payload.getFileName());
        summaryResponse.setHeader(payload.getHeader());
        summaryResponse.setMetadata(payload.getMetadata());
        summaryResponse.setPayload(payload.getPayload());
        summaryResponse.setSummaryFileURL(payload.getSummaryFileURL());

        return new ApiResponse("Success", "success", summaryResponse);

    } catch (Exception ex) {
        logger.error("❌ Processing failed", ex);
        return new ApiResponse("Processing failed", "error", null);
    }
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
                if (parts.length >= 2) {
                    String account = parts[0].trim();
                    String customer = parts[1].trim();
                    if (!successMap.containsKey(account)) {
                        SummaryProcessedFile failEntry = new SummaryProcessedFile();
                        failEntry.setAccountNumber(account);
                        failEntry.setCustomerId(customer);
                        failEntry.setStatusCode("FAILURE");
                        failEntry.setStatusDescription("Processing failed");
                        failures.add(failEntry);
                    }
                }
            }
        } catch (IOException e) {
            logger.error("❌ Failed to read error report file", e);
        }
        return failures;
    }

    private Map<String, String> extractAccountCustomerMapFromDoc(Document doc) {
        Map<String, String> map = new HashMap<>();
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
                if ("AccountNumber".equalsIgnoreCase(keyName)) account = keyValue;
                if ("CISNumber".equalsIgnoreCase(keyName)) customer = keyValue;
            }
            if (account != null && customer != null) map.put(account, customer);
        }
        return map;
    }

    private List<PrintFile> uploadPrintFiles(Path jobDir, KafkaMessage msg) {
        List<PrintFile> printFiles = new ArrayList<>();
        Path printFolder = jobDir.resolve("print");
        if (!Files.exists(printFolder)) return printFiles;

        try (Stream<Path> paths = Files.list(printFolder)) {
            paths.filter(Files::isRegularFile).forEach(file -> {
                try {
                    String fileName = file.getFileName().toString();
                    String remotePath = String.format("%s/%s/%s/print/%s",
                            msg.getSourceSystem(), msg.getBatchId(), msg.getUniqueConsumerRef(), fileName);

                    String blobUrl = blobStorageService.uploadFile(file.toFile(), remotePath);
                    printFiles.add(new PrintFile(decodeUrl(blobUrl)));
                } catch (Exception e) {
                    logger.error("Failed to upload print file: {}", file, e);
                }
            });
        } catch (IOException e) {
            logger.error("Error accessing print folder", e);
        }
        return printFiles;
    }

    private OTResponse callOrchestrationBatchApi(String token, KafkaMessage msg) {
        try {
            HttpHeaders headers = new HttpHeaders();
            headers.set("Authorization", "Bearer " + token);
            headers.setContentType(MediaType.APPLICATION_JSON);

            HttpEntity<String> request = new HttpEntity<>(objectMapper.writeValueAsString(msg), headers);
            ResponseEntity<Map> response = restTemplate.exchange(otOrchestrationApiUrl, HttpMethod.POST, request, Map.class);

            logger.info("📨 OT Orchestration Response: {}", objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(response.getBody()));

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

                // Check non-empty
                if (xmlFile.length() == 0) {
                    logger.info("⏳ XML file found but still empty. Waiting...");
                    TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
                    continue;
                }

                // Try parsing to ensure it’s complete
                try {
                    DocumentBuilderFactory dbFactory = DocumentBuilderFactory.newInstance();
                    DocumentBuilder dBuilder = dbFactory.newDocumentBuilder();
                    dBuilder.parse(xmlFile);  // Attempt parse
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

    private void writeAndUploadMetadataJson(KafkaMessage message, Path jobDir) {
        try {
            Map<String, Object> metaMap = objectMapper.convertValue(message, Map.class);
            if (metaMap.containsKey("batchFiles")) {
                List<Map<String, Object>> files = (List<Map<String, Object>>) metaMap.get("batchFiles");
                for (Map<String, Object> f : files) {
                    Object blob = f.remove("blobUrl");
                    if (blob != null) {
                        f.put("MountPath", blob);
                    }
                }
            }

            String json = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(metaMap);
            logger.info("📝 Metadata JSON before sending to OT:\n{}", json);

            File metaFile = new File(jobDir.toFile(), "metadata.json");
            FileUtils.writeStringToFile(metaFile, json, StandardCharsets.UTF_8);
            String blobPath = String.format("%s/Trigger/metadata_%s.json", message.getSourceSystem(), message.getBatchId());
            blobStorageService.uploadFile(metaFile.getAbsolutePath(), blobPath);
            FileUtils.forceDelete(metaFile);
        } catch (Exception ex) {
            logger.error("metadata.json generation failed", ex);
        }
    }

    private List<SummaryProcessedFile> buildAndUploadProcessedFiles(Path jobDir, Map<String, String> accountCustomerMap, KafkaMessage msg) throws IOException {
        List<SummaryProcessedFile> list = new ArrayList<>();
        List<String> folders = List.of("archive", "email", "html", "mobstat", "txt");

        for (String folder : folders) {
            Path subDir = jobDir.resolve(folder);
            if (!Files.exists(subDir)) continue;

            Files.list(subDir).filter(Files::isRegularFile).forEach(file -> {
                try {
                    String fileName = file.getFileName().toString();
                    String account = extractAccountFromFileName(fileName);
                    if (account == null) return;

                    String customer = accountCustomerMap.get(account);

                    SummaryProcessedFile entry = list.stream()
                            .filter(e -> account.equals(e.getAccountNumber()))
                            .findFirst()
                            .orElseGet(() -> {
                                SummaryProcessedFile newEntry = new SummaryProcessedFile();
                                newEntry.setAccountNumber(account);
                                newEntry.setCustomerId(customer);
                                newEntry.setStatusCode("OK");
                                newEntry.setStatusDescription("Success");
                                list.add(newEntry);
                                return newEntry;
                            });

                    String blobUrl = blobStorageService.uploadFile(
                            file, // use Path for binary-safe upload
                            String.format("%s/%s/%s/%s/%s",
                                    msg.getSourceSystem(),
                                    msg.getBatchId(),
                                    msg.getUniqueConsumerRef(),
                                    folder,
                                    fileName)
                    );

                    // Decode URL before storing
                    String decodedUrl = decodeUrl(blobUrl);

                    switch (folder) {
                        case "archive" -> entry.setPdfArchiveFileUrl(decodedUrl);
                        case "email" -> entry.setPdfEmailFileUrl(decodedUrl);
                        case "html" -> entry.setHtmlEmailFileUrl(decodedUrl);
                        case "txt" -> entry.setTxtEmailFileUrl(decodedUrl);
                        case "mobstat" -> entry.setPdfMobstatFileUrl(decodedUrl);
                    }

                } catch (Exception e) {
                    logger.error("Error uploading file: {}", file, e);
                }
            });
        }
        return list;
    }

    private List<SummaryProcessedFile> appendFailureEntries(Path jobDir, KafkaMessage msg, Map<String, String> successMap) {
        Map<String, String> failureMap = new HashMap<>();
        Path errorReportPath = jobDir.resolve("ErrorReport.csv");
        if (Files.exists(errorReportPath)) {
            try (BufferedReader reader = Files.newBufferedReader(errorReportPath)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] fields = line.split(",");
                    if (fields.length >= 2) {
                        String account = fields[0].trim();
                        String customer = fields[1].trim();
                        failureMap.put(account, customer);
                    }
                }
            } catch (IOException e) {
                logger.error("❌ Failed to read ErrorReport.csv", e);
            }
        }

        List<SummaryProcessedFile> failures = new ArrayList<>();
        for (Map.Entry<String, String> entry : failureMap.entrySet()) {
            String account = entry.getKey();
            if (!successMap.containsKey(account)) {
                SummaryProcessedFile failEntry = new SummaryProcessedFile();
                failEntry.setAccountNumber(account);
                failEntry.setCustomerId(entry.getValue());
                failEntry.setStatusCode("FAILURE");
                failEntry.setStatusDescription("Processing failed");
                failures.add(failEntry);
            }
        }
        return failures;
    }

    private String extractAccountFromFileName(String fileName) {
        Matcher matcher = Pattern.compile("(\\d{9,})").matcher(fileName);
        return matcher.find() ? matcher.group(1) : null;
    }

    private String decodeUrl(String encodedUrl) {
        try {
            return URLDecoder.decode(encodedUrl, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return encodedUrl;
        }
    }
}

class OTResponse {
    private String jobId;
    private String id;

    public String getJobId() { return jobId; }
    public void setJobId(String jobId) { this.jobId = jobId; }

    public String getId() { return id; }
    public void setId(String id) { this.id = id; }
}
