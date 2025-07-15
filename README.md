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
            KafkaMessage message = objectMapper.readValue(rawMessage, KafkaMessage.class);
            String batchId = message.getBatchId();
            Path batchDir = Paths.get(mountPath, "input", message.getSourceSystem(), batchId);
            Files.createDirectories(batchDir);

            for (BatchFile file : message.getBatchFiles()) {
                String blobUrl = file.getBlobUrl();
                String content = blobStorageService.downloadFileContent(blobUrl);
                Path localPath = batchDir.resolve(file.getFilename());
                Files.write(localPath, content.getBytes(StandardCharsets.UTF_8));
                file.setBlobUrl(localPath.toString());
            }

            String url;
            switch (message.getSourceSystem().toUpperCase()) {
                case "DEBTMAN" -> url = otOrchestrationApiUrl;
                case "MFC" -> url = orchestrationMfcUrl;
                default -> throw new IllegalArgumentException("Unsupported source system: " + message.getSourceSystem());
            }

            OTResponse otResponse = callOrchestrationBatchApi(orchestrationAuthToken, url, message);

            if (otResponse == null) {
                kafkaTemplate.send(kafkaOutputTopic, "{\"status\":\"FAILURE\",\"message\":\"OT call failed\"}");
                ack.acknowledge();
                return;
            }

            kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(Map.of(
                    "batchID", batchId,
                    "status", "PENDING",
                    "message", "OT Request Sent"
            )));
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

            Document doc = DocumentBuilderFactory.newInstance().newDocumentBuilder().parse(xmlFile);
            doc.getDocumentElement().normalize();

            Map<String, String> accountCustomerMap = extractAccountCustomerMapFromDoc(doc);
            Map<String, SummaryProcessedFile> customerMap = new HashMap<>();
            accountCustomerMap.forEach((acc, cus) -> {
                SummaryProcessedFile spf = new SummaryProcessedFile();
                spf.setAccountNumber(acc);
                spf.setCustomerId(cus);
                customerMap.put(acc, spf);
            });

            Map<String, Map<String, String>> errorMap = parseErrorReport(message);

            Path jobDir = Paths.get(mountPath, "output", message.getSourceSystem(), otResponse.getJobId());
            List<SummaryProcessedFile> processedFiles = buildDetailedProcessedFiles(jobDir, customerMap, errorMap, message);

            List<PrintFile> printFiles = uploadPrintFiles(jobDir, message);

            SummaryPayload payload = SummaryJsonWriter.buildPayload(message, processedFiles, printFiles, null, accountCustomerMap.size());
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

            kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(
                    new ApiResponse("Summary generated", "COMPLETED", response)));

        } catch (Exception e) {
            logger.error("❌ Error post-OT summary generation", e);
        }
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
                    String method = parts[2].trim().toUpperCase();
                    String status = parts[4].trim();
                    map.computeIfAbsent(acc, k -> new HashMap<>()).put(method, status);
                }
            }
        } catch (Exception e) {
            logger.warn("Error reading ErrorReport", e);
        }
        return map;
    }

    private List<SummaryProcessedFile> buildDetailedProcessedFiles(Path jobDir, Map<String, SummaryProcessedFile> customerMap, Map<String, Map<String, String>> errorMap, KafkaMessage msg) throws IOException {
        List<SummaryProcessedFile> result = new ArrayList<>();
        Set<String> folders = Set.of("email", "archive", "html", "mobstat", "txt");

        for (Map.Entry<String, SummaryProcessedFile> entry : customerMap.entrySet()) {
            String account = entry.getKey();
            SummaryProcessedFile spf = entry.getValue();

            for (String folder : folders) {
                Path folderPath = jobDir.resolve(folder);
                if (Files.exists(folderPath)) {
                    Optional<Path> fileOpt = Files.list(folderPath)
                            .filter(p -> p.getFileName().toString().contains(account))
                            .findFirst();

                    if (fileOpt.isPresent()) {
                        Path file = fileOpt.get();
                        String blobUrl = blobStorageService.uploadFile(file.toFile(), msg.getSourceSystem() + "/" + msg.getBatchId() + "/" + folder + "/" + file.getFileName());
                        String decoded = decodeUrl(blobUrl);

                        switch (folder) {
                            case "email" -> {
                                spf.setPdfEmailFileUrl(decoded);
                                spf.setPdfEmailStatus("OK");
                            }
                            case "archive" -> {
                                spf.setPdfArchiveFileUrl(decoded);
                                spf.setPdfArchiveStatus("OK");
                            }
                            case "html" -> {
                                spf.setHtmlEmailFileUrl(decoded);
                                spf.setHtmlEmailStatus("OK");
                            }
                            case "mobstat" -> {
                                spf.setPdfMobstatFileUrl(decoded);
                                spf.setPdfMobstatStatus("OK");
                            }
                            case "txt" -> {
                                spf.setTxtEmailFileUrl(decoded);
                                spf.setTxtEmailStatus("OK");
                            }
                        }
                    } else {
                        Map<String, String> err = errorMap.get(account);
                        if (err != null) {
                            String outputMethod = folder.toUpperCase();
                            String status = err.getOrDefault(outputMethod, "");
                            if (status.equalsIgnoreCase("Failed")) {
                                switch (folder) {
                                    case "email" -> spf.setPdfEmailStatus("Failed");
                                    case "archive" -> spf.setPdfArchiveStatus("Failed");
                                    case "html" -> spf.setHtmlEmailStatus("Failed");
                                    case "mobstat" -> spf.setPdfMobstatStatus("Failed");
                                    case "txt" -> spf.setTxtEmailStatus("Failed");
                                }
                            }
                        }
                    }
                }
            }

            boolean hasFailure = Stream.of(
                    spf.getPdfEmailStatus(), spf.getPdfArchiveStatus(), spf.getHtmlEmailStatus(),
                    spf.getPdfMobstatStatus(), spf.getTxtEmailStatus()
            ).anyMatch(s -> "NOT-OK".equalsIgnoreCase(s));

            boolean allNull = Stream.of(
                    spf.getPdfEmailStatus(), spf.getPdfArchiveStatus(), spf.getHtmlEmailStatus(),
                    spf.getPdfMobstatStatus(), spf.getTxtEmailStatus()
            ).allMatch(Objects::isNull);

            if (allNull) {
                spf.setStatusCode("FAILURE");
                spf.setStatusDescription("No files processed");
            } else if (hasFailure) {
                spf.setStatusCode("PARTIAL");
                spf.setStatusDescription("Some files missing");
            } else {
                spf.setStatusCode("OK");
                spf.setStatusDescription("Success");
            }

            result.add(spf);
        }

        return result;
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

    private File waitForXmlFile(String jobId, String id) throws InterruptedException {
        Path docgenRoot = Paths.get(mountPath, "jobs", jobId, id, "docgen");
        long startTime = System.currentTimeMillis();
        while ((System.currentTimeMillis() - startTime) < rptMaxWaitSeconds * 1000L) {
            if (Files.exists(docgenRoot)) {
                try (Stream<Path> paths = Files.walk(docgenRoot)) {
                    Optional<Path> xmlPath = paths
                            .filter(Files::isRegularFile)
                            .filter(p -> p.getFileName().toString().equalsIgnoreCase("_STDDELIVERYFILE.xml"))
                            .findFirst();
                    if (xmlPath.isPresent()) return xmlPath.get().toFile();
                } catch (IOException e) {
                    logger.warn("Error scanning docgen", e);
                }
            }
            TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
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

    private String decodeUrl(String url) {
        try {
            return URLDecoder.decode(url, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return url;
        }
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
