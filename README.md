package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.*;
import com.nedbank.kafka.filemanage.utils.SummaryJsonWriter;
import jakarta.annotation.PreDestroy;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.*;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;

@Service
public class KafkaListenerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    @Value("${kafka.output.topic}")
    private String outputTopic;

    @Autowired private FileGenerator fileGenerator;
    @Autowired private BlobStorageService blobStorageService;
    @Autowired private SummaryJsonWriter summaryJsonWriter;
    @Autowired private KafkaTemplate<String, String> kafkaTemplate;
    @Autowired private ObjectMapper objectMapper;

    // ✅ Process after OT, generate summary with URLs
    public void processAfterOT(KafkaMessage kafkaMessage, OTResponse otResponse) {
        try {
            logger.info("⏳ Waiting for XML for jobId={}, id={}", otResponse.getJobId(), otResponse.getId());
            File xmlFile = waitForXmlFile(otResponse.getJobId(), otResponse.getId());

            if (xmlFile == null) {
                throw new IllegalStateException("XML not found");
            }

            logger.info("✅ Found XML file: {}", xmlFile.getAbsolutePath());

            Path jobDir = xmlFile.getParentFile().toPath();
            logger.info("📁 Job Directory: {}", jobDir);

            List<SummaryProcessedFile> customerList = fileGenerator.extractCustomerListFromXML(xmlFile);
            Map<String, Map<String, String>> errorMap = fileGenerator.extractErrorMap(jobDir);

            // ✅ Build processed + print files with real blob URLs
            List<SummaryProcessedFile> processedFiles = buildDetailedProcessedFiles(jobDir, customerList, errorMap, kafkaMessage);
            List<SummaryPrintFile> printFiles = fileGenerator.buildPrintFiles(jobDir, kafkaMessage);

            // ✅ Generate summary payload
            SummaryPayload payload = summaryJsonWriter.buildPayload(kafkaMessage, processedFiles, printFiles);
            String summaryBlobUrl = blobStorageService.uploadSummaryJson(payload, kafkaMessage);

            // ✅ Beautified JSON log
            String prettyJson = objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(payload);
            logger.info("📄 Final Summary Payload:\n{}", prettyJson);

            // ✅ Send Kafka message
            kafkaTemplate.send(outputTopic, summaryBlobUrl);
            logger.info("📤 Sent Kafka message to outputTopic={} with summary URL", outputTopic);

        } catch (Exception e) {
            logger.error("❌ Error in processAfterOT for jobId={}, id={}", otResponse.getJobId(), otResponse.getId(), e);
        }
    }

    // ✅ Build processed files per customer/outputMethod
    private List<SummaryProcessedFile> buildDetailedProcessedFiles(
            Path jobDir,
            List<SummaryProcessedFile> customerList,
            Map<String, Map<String, String>> errorMap,
            KafkaMessage msg) throws IOException {

        List<SummaryProcessedFile> finalList = new ArrayList<>();
        Map<String, SummaryProcessedFile> outputMap = new LinkedHashMap<>();

        List<String> folders = List.of("email", "archive", "mobstat", "print");
        Map<String, String> folderToOutputMethod = Map.of(
                "email", "EMAIL",
                "archive", "ARCHIVE",
                "mobstat", "MOBSTAT",
                "print", "PRINT"
        );

        for (SummaryProcessedFile spf : customerList) {
            String customer = spf.getCustomer();
            String account = spf.getAccount();
            for (String folder : folders) {
                String outputMethod = folderToOutputMethod.get(folder);
                String fileName = customer + "_" + account + ".pdf";
                Path filePath = jobDir.resolve(folder).resolve(fileName);

                SummaryProcessedFile entry = new SummaryProcessedFile();
                entry.setCustomer(customer);
                entry.setAccount(account);
                entry.setOutputMethod(outputMethod);

                if (Files.exists(filePath)) {
                    String blobUrl = blobStorageService.uploadFileAndReturnLocation(filePath.toFile(), msg, folder);
                    entry.setBlobURL(blobUrl);
                    entry.setStatus("SUCCESS");
                    logger.info("✅ Uploaded file for customer={}, method={}, url={}", customer, outputMethod, blobUrl);
                } else {
                    entry.setStatus("FAILED");
                    entry.setBlobURL(null);
                    logger.warn("❌ File not found for customer={}, method={}, path={}", customer, outputMethod, filePath);
                }

                outputMap.put(customer + "::" + account + "::" + outputMethod, entry);
            }
        }

        finalList.addAll(outputMap.values());
        return finalList;
    }

    // Placeholder (you likely already have it)
    private File waitForXmlFile(String jobId, String id) {
        // Logic to wait/poll for XML availability
        return new File("/mnt/data/jobs/" + jobId + "/" + id + "/output.xml");
    }

    @PreDestroy
    public void shutdown() {
        logger.info("🔻 KafkaListenerService shutting down.");
    }
}
