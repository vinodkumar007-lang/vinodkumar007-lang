public class AccountSummary {
    private String accountNumber;
    private String statusCode;
    private String statusDescription;
    private String pdfEmailStatus;
    private String pdfArchiveStatus;
    private String pdfMobstatStatus;
    private String printStatus;
    // Getters and setters
}

package com.nedbank.kafka.filemanage.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.nedbank.kafka.filemanage.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.file.*;
import java.time.Instant;
import java.util.*;

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

    public static SummaryPayload buildPayload(KafkaMessage message,
                                              List<SummaryProcessedFile> processedFiles,
                                              int pagesProcessed,
                                              List<PrintFile> printFiles,
                                              String mobstatTriggerPath,
                                              int customersProcessed) {

        SummaryPayload payload = new SummaryPayload();

        payload.setBatchID(message.getBatchId());
        payload.setFileName(message.getBatchId() + ".csv");
        payload.setMobstatTriggerFile(mobstatTriggerPath);

        // Header block
        Header header = new Header();
        header.setTenantCode(message.getTenantCode());
        header.setChannelID(message.getChannelID());
        header.setAudienceID(message.getAudienceID());
        header.setSourceSystem(message.getSourceSystem());
        header.setProduct(message.getProduct());
        header.setJobName(message.getJobName());
        header.setTimestamp(Instant.now().toString());
        payload.setHeader(header);

        // Determine overall status
        String overallStatus = "Completed";
        if (processedFiles != null && !processedFiles.isEmpty()) {
            boolean allFailed = processedFiles.stream().allMatch(f -> "FAILURE".equalsIgnoreCase(f.getStatusCode()));
            boolean anyFailed = processedFiles.stream().anyMatch(f -> "FAILURE".equalsIgnoreCase(f.getStatusCode()) || "PARTIAL".equalsIgnoreCase(f.getStatusCode()));

            if (allFailed) overallStatus = "Failure";
            else if (anyFailed) overallStatus = "Partial";
        }

        // Metadata block
        Metadata metadata = new Metadata();
        metadata.setTotalFilesProcessed(customersProcessed);
        metadata.setProcessingStatus(overallStatus);
        metadata.setEventOutcomeCode("0");
        metadata.setEventOutcomeDescription("Success");
        metadata.setCustomerSummaries(buildCustomerSummaries(processedFiles)); // ✅ added
        payload.setMetadata(metadata);

        // Payload block
        Payload payloadDetails = new Payload();
        payloadDetails.setUniqueConsumerRef(message.getUniqueConsumerRef());
        payloadDetails.setUniqueECPBatchRef(message.getUniqueECPBatchRef());
        payloadDetails.setRunPriority(message.getRunPriority());
        payloadDetails.setEventID(message.getEventID());
        payloadDetails.setEventType(message.getEventType());
        payloadDetails.setRestartKey(message.getRestartKey());
        payloadDetails.setFileCount(pagesProcessed);
        payload.setPayload(payloadDetails);

        // Processed files
        payload.setProcessedFiles(processedFiles != null ? processedFiles : new ArrayList<>());

        // Print files
        payload.setPrintFiles(printFiles != null ? printFiles : new ArrayList<>());

        return payload;
    }

    private static List<CustomerSummary> buildCustomerSummaries(List<SummaryProcessedFile> processedFiles) {
        Map<String, CustomerSummary> customerMap = new LinkedHashMap<>();

        for (SummaryProcessedFile file : processedFiles) {
            String customerId = file.getCustomerId();
            String account = file.getAccountNumber();
            if (customerId == null || account == null) continue;

            CustomerSummary customerSummary = customerMap.computeIfAbsent(customerId, id -> {
                CustomerSummary cs = new CustomerSummary();
                cs.setCustomerId(id);
                cs.setAccounts(new ArrayList<>());
                return cs;
            });

            // Check if account already added
            boolean alreadyAdded = customerSummary.getAccounts().stream()
                    .anyMatch(a -> account.equals(a.getAccountNumber()));

            if (!alreadyAdded) {
                AccountSummary acc = new AccountSummary();
                acc.setAccountNumber(account);
                acc.setStatusCode(file.getStatusCode());
                acc.setStatusDescription(file.getStatusDescription());
                acc.setPdfEmailStatus(file.getPdfEmailStatus());
                acc.setPdfArchiveStatus(file.getPdfArchiveStatus());
                acc.setPdfMobstatStatus(file.getPdfMobstatStatus());
                acc.setPrintStatus(file.getPrintStatus());

                customerSummary.getAccounts().add(acc);
            }
        }

        return new ArrayList<>(customerMap.values());
    }
}
