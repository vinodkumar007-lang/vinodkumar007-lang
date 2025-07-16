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

    private List<SummaryProcessedFile> buildDetailedProcessedFiles(
            Path jobDir,
            Map<String, SummaryProcessedFile> customerMap,
            Map<String, Map<String, String>> errorMap,
            KafkaMessage msg) throws IOException {

        List<SummaryProcessedFile> result = new ArrayList<>();
        List<String> folders = List.of("email", "archive", "mobstat", "print");
        Map<String, String> folderToOutputMethod = Map.of(
                "email", "EMAIL",
                "archive", "ARCHIVE",
                "mobstat", "MOBSTAT",
                "print", "PRINT"
        );

        for (Map.Entry<String, SummaryProcessedFile> entry : customerMap.entrySet()) {
            String account = entry.getKey();
            SummaryProcessedFile spf = entry.getValue();
            Map<String, Boolean> methodFound = new HashMap<>();

            for (String folder : folders) {
                Path folderPath = jobDir.resolve(folder);
                Optional<Path> fileOpt = Files.exists(folderPath)
                        ? Files.list(folderPath)
                        .filter(p -> p.getFileName().toString().contains(account))
                        .findFirst()
                        : Optional.empty();

                String outputMethod = folderToOutputMethod.get(folder);
                Map<String, String> errorEntry = errorMap.getOrDefault(account, Collections.emptyMap());
                String failureStatus = errorEntry.getOrDefault(outputMethod, "");

                boolean fileFound = fileOpt.isPresent();
                methodFound.put(outputMethod, fileFound);

                if (fileFound) {
                    Path file = fileOpt.get();
                    String blobUrl = blobStorageService.uploadFile(
                            file.toFile(),
                            msg.getSourceSystem() + "/" + msg.getBatchId() + "/" + folder + "/" + file.getFileName()
                    );
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
                        case "mobstat" -> {
                            spf.setPdfMobstatFileUrl(decoded);
                            spf.setPdfMobstatStatus("OK");
                        }
                        case "print" -> spf.setPrintFileURL(decoded);
                    }
                } else {
                    // file not found, check error report
                    boolean isExplicitlyFailed = "Failed".equalsIgnoreCase(failureStatus);
                    if (isExplicitlyFailed) {
                        switch (folder) {
                            case "email" -> spf.setPdfEmailStatus("Failed");
                            case "archive" -> spf.setPdfArchiveStatus("Failed");
                            case "mobstat" -> spf.setPdfMobstatStatus("Failed");
                        }
                    } else {
                        // if not in error report, leave status as empty
                        switch (folder) {
                            case "email" -> spf.setPdfEmailStatus("");
                            case "archive" -> spf.setPdfArchiveStatus("");
                            case "mobstat" -> spf.setPdfMobstatStatus("");
                        }
                    }
                }
            }

            // Final status decision logic
            List<String> statuses = Arrays.asList(
                    spf.getPdfEmailStatus(),
                    spf.getPdfArchiveStatus(),
                    spf.getPdfMobstatStatus()
            );

            long failedCount = statuses.stream().filter("Failed"::equalsIgnoreCase).count();
            long knownCount = statuses.stream().filter(s -> s != null && !s.isBlank()).count();

            if (failedCount == knownCount && knownCount > 0) {
                spf.setStatusCode("FAILED");
                spf.setStatusDescription("All methods failed");
            } else if (failedCount > 0) {
                spf.setStatusCode("PARTIAL");
                spf.setStatusDescription("Some methods failed");
            } else {
                spf.setStatusCode("SUCCESS");
                spf.setStatusDescription("Success");
            }

            result.add(spf);
        }

        return result;
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

    private String decodeUrl(String url) {
        try {
            return URLDecoder.decode(url, StandardCharsets.UTF_8);
        } catch (Exception e) {
            return url;
        }
    }
}
