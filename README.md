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
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;
import org.springframework.web.client.RestTemplate;

import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.regex.*;

@Service
public class KafkaListenerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    @Value("${mount.path}")
    private String mountPath;

    @Value("${opentext.api.url}")
    private String opentextApiUrl;

    @Value("${otds.token.url}")
    private String otdsTokenUrl;

    @Value("${otds.username}")
    private String otdsUsername;

    @Value("${otds.password}")
    private String otdsPassword;

    @Value("${otds.client-id}")
    private String otdsClientId;

    @Value("${otds.client-secret}")
    private String otdsClientSecret;

    @Value("${kafka.topic.output}")
    private String kafkaOutputTopic;

    @Value("${rpt.max.wait.seconds}")
    private int rptMaxWaitSeconds;

    @Value("${rpt.poll.interval.millis}")
    private int rptPollIntervalMillis;

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
            logger.info("📩 Received Kafka message.");
            KafkaMessage kafkaMessage = objectMapper.readValue(message, KafkaMessage.class);
            ApiResponse response = processSingleMessage(kafkaMessage);
            kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(response));
            logger.info("✅ Sent processed response to Kafka output topic.");
        } catch (Exception ex) {
            logger.error("❌ Error processing Kafka message", ex);
        }
    }

    private ApiResponse processSingleMessage(KafkaMessage message) {
        try {
            List<BatchFile> batchFiles = message.getBatchFiles();
            if (batchFiles == null || batchFiles.isEmpty()) {
                return new ApiResponse("No batch files found", "error", null);
            }

            long dataCount = batchFiles.stream().filter(f -> "DATA".equalsIgnoreCase(f.getFileType())).count();
            long refCount = batchFiles.stream().filter(f -> "REF".equalsIgnoreCase(f.getFileType())).count();
            boolean hasInvalid = batchFiles.stream().anyMatch(f -> f.getFileType() == null || f.getFileType().trim().isEmpty());

            if (hasInvalid) return new ApiResponse("Invalid or empty fileType found", "error", null);
            if (dataCount == 0) return new ApiResponse("No DATA files to process", "error", null);
            if (dataCount > 1 && refCount == 0) return new ApiResponse("Too many DATA files without REF", "error", null);

            List<BatchFile> dataFilesOnly = batchFiles.stream().filter(f -> "DATA".equalsIgnoreCase(f.getFileType())).toList();
            message.setBatchFiles(dataFilesOnly);

            String batchId = message.getBatchId();
            String guiRef = message.getUniqueConsumerRef();
            Path jobDir = Paths.get(mountPath, "output", "DEBTMAN", batchId);
            Files.createDirectories(jobDir);

            for (BatchFile file : message.getBatchFiles()) {
                String blobUrl = file.getBlobUrl();
                String content = blobStorageService.downloadFileContent(blobUrl);
                Path localPath = jobDir.resolve(extractFileName(blobUrl));
                Files.write(localPath, content.getBytes(StandardCharsets.UTF_8));
                file.setBlobUrl(localPath.toString());
            }

            writeAndUploadMetadataJson(message, jobDir);

            String token = fetchAccessToken();
            sendToOpenText(token, message);

            logger.info("⏳ Waiting for .rpt file every {}ms, up to {}s...", rptPollIntervalMillis, rptMaxWaitSeconds);
            File rptFile = waitForRptFile(jobDir);
            if (rptFile == null) return new ApiResponse("Timeout waiting for .rpt", "error", null);

            Map<String, String> accountCustomerMap = extractAccountCustomerMapFromRpt(rptFile);

            List<SummaryProcessedFile> processedFiles = buildProcessedFiles(jobDir, accountCustomerMap);
            List<SummaryPrintFile> printFiles = buildPrintFiles(jobDir);
            String mobstatTriggerPath = jobDir.resolve("mobstat_trigger/DropData.trigger").toString();

            SummaryPayload payload = SummaryJsonWriter.buildPayload(message, processedFiles, printFiles, mobstatTriggerPath);
            String summaryPath = SummaryJsonWriter.writeSummaryJsonToFile(payload);
            String summaryUrl = blobStorageService.uploadSummaryJson(summaryPath, message, "summary_" + batchId + ".json");
            payload.setSummaryFileURL(decodeUrl(summaryUrl));

            return new ApiResponse("Success", "success", new SummaryResponse(payload));
        } catch (Exception ex) {
            logger.error("❌ Failed in processing", ex);
            return new ApiResponse("Processing failed: " + ex.getMessage(), "error", null);
        }
    }

    private List<SummaryProcessedFile> buildProcessedFiles(Path jobDir, Map<String, String> accountCustomerMap) throws IOException {
        List<SummaryProcessedFile> list = new ArrayList<>();
        List<String> folderNames = List.of("archive", "email", "html", "mobstat", "txt");

        for (String folder : folderNames) {
            Path subDir = jobDir.resolve(folder);
            if (!Files.exists(subDir)) continue;

            Files.list(subDir).forEach(file -> {
                String fileName = file.getFileName().toString();
                String account = extractAccountFromFileName(fileName);
                if (account == null) return;
                String customer = accountCustomerMap.get(account);
                SummaryProcessedFile entry = list.stream().filter(e -> account.equals(e.getAccountNumber())).findFirst().orElseGet(() -> {
                    SummaryProcessedFile newEntry = new SummaryProcessedFile();
                    newEntry.setAccountNumber(account);
                    newEntry.setCustomerId(customer);
                    newEntry.setStatusCode("OK");
                    newEntry.setStatusDescription("Success");
                    list.add(newEntry);
                    return newEntry;
                });
                String blobUrl = file.toUri().toString();
                switch (folder) {
                    case "archive" -> entry.setPdfArchiveFileUrl(blobUrl);
                    case "email" -> entry.setPdfEmailFileUrl(blobUrl);
                    case "html" -> entry.setHtmlEmailFileUrl(blobUrl);
                    case "txt" -> entry.setTxtEmailFileUrl(blobUrl);
                    case "mobstat" -> entry.setPdfMobstatFileUrl(blobUrl);
                }
            });
        }
        return list;
    }

    private List<SummaryPrintFile> buildPrintFiles(Path jobDir) throws IOException {
        List<SummaryPrintFile> list = new ArrayList<>();
        Path printDir = jobDir.resolve("print");
        if (!Files.exists(printDir)) return list;

        Files.list(printDir).forEach(file -> {
            SummaryPrintFile print = new SummaryPrintFile();
            print.setPrintFileURL(file.toUri().toString());
            list.add(print);
        });
        return list;
    }
