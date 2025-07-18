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

        Header header = new Header();
        header.setTenantCode(message.getTenantCode());
        header.setChannelID(message.getChannelID());
        header.setAudienceID(message.getAudienceID());
        header.setSourceSystem(message.getSourceSystem());
        header.setProduct(message.getProduct());
        header.setJobName(message.getJobName());
        header.setTimestamp(Instant.now().toString());
        payload.setHeader(header);

        String overallStatus = "Completed";
        if (processedFiles != null && !processedFiles.isEmpty()) {
            boolean allFailed = processedFiles.stream().allMatch(f -> "FAILURE".equalsIgnoreCase(f.getStatusCode()));
            boolean anyFailed = processedFiles.stream().anyMatch(f ->
                    "FAILURE".equalsIgnoreCase(f.getStatusCode()) || "PARTIAL".equalsIgnoreCase(f.getStatusCode()));
            if (allFailed) overallStatus = "Failure";
            else if (anyFailed) overallStatus = "Partial";
        }

        Metadata metadata = new Metadata();
        metadata.setTotalFilesProcessed(customersProcessed);
        metadata.setProcessingStatus(overallStatus);
        metadata.setEventOutcomeCode("0");
        metadata.setEventOutcomeDescription("Success");
        payload.setMetadata(metadata);

        Payload payloadDetails = new Payload();
        payloadDetails.setUniqueConsumerRef(message.getUniqueConsumerRef());
        payloadDetails.setUniqueECPBatchRef(message.getUniqueECPBatchRef());
        payloadDetails.setRunPriority(message.getRunPriority());
        payloadDetails.setEventID(message.getEventID());
        payloadDetails.setEventType(message.getEventType());
        payloadDetails.setRestartKey(message.getRestartKey());
        payloadDetails.setFileCount(pagesProcessed);
        payload.setPayload(payloadDetails);
        payload.setProcessedFiles(processedFiles);
        //List<CustomerSummary> customerSummaries = buildCustomerSummaries(processedFiles);
        //payload.setCustomerSummaries(customerSummaries);

        // Optional: include printFiles if needed
        payload.setPrintFiles(printFiles);

        return payload;
    }

   /* private static List<CustomerSummary> buildCustomerSummaries(List<SummaryProcessedFile> processedFiles) {
        List<CustomerSummary> resultList = new ArrayList<>();

        Map<String, Map<String, List<SummaryProcessedFile>>> grouped = new HashMap<>();

        for (SummaryProcessedFile file : processedFiles) {
            if (file.getCustomerId() == null || file.getAccountNumber() == null) continue;

            grouped
                    .computeIfAbsent(file.getCustomerId(), k -> new HashMap<>())
                    .computeIfAbsent(file.getAccountNumber(), k -> new ArrayList<>())
                    .add(file);
        }

        for (Map.Entry<String, Map<String, List<SummaryProcessedFile>>> customerEntry : grouped.entrySet()) {
            String customerId = customerEntry.getKey();
            Map<String, List<SummaryProcessedFile>> accountMap = customerEntry.getValue();

            CustomerSummary customerSummary = new CustomerSummary();
            customerSummary.setCustomerId(customerId);

            int totalAccounts = 0;
            int totalSuccess = 0;
            int totalFailures = 0;

            for (Map.Entry<String, List<SummaryProcessedFile>> accountEntry : accountMap.entrySet()) {
                String accountNumber = accountEntry.getKey();
                List<SummaryProcessedFile> files = accountEntry.getValue();

                AccountSummary acc = new AccountSummary();
                acc.setAccountNumber(accountNumber);

                for (SummaryProcessedFile file : files) {
                    String method = file.getOutputMethod() != null ? file.getOutputMethod().toUpperCase() : "";
                    String status = file.getStatus();
                    String url = file.getBlobURL();

                    switch (method) {
                        case "EMAIL" -> {
                            acc.setPdfEmailStatus(status);
                            acc.setPdfEmailBlobUrl(url);
                        }
                        case "ARCHIVE" -> {
                            acc.setPdfArchiveStatus(status);
                            acc.setPdfArchiveBlobUrl(url);
                        }
                        case "MOBSTAT" -> {
                            acc.setPdfMobstatStatus(status);
                            acc.setPdfMobstatBlobUrl(url);
                        }
                        case "PRINT" -> {
                            acc.setPrintStatus(status);
                            acc.setPrintBlobUrl(url);
                        }
                        default -> logger.warn("❗ Unrecognized output method: {}", method);
                    }

                    if ("SUCCESS".equalsIgnoreCase(status)) totalSuccess++;
                    else totalFailures++;
                }

                customerSummary.getAccounts().add(acc);
                totalAccounts++;
            }

            //customerSummary.setTotalAccounts(totalAccounts);
            //customerSummary.setTotalSuccess(totalSuccess);
            //customerSummary.setTotalFailure(totalFailures);

            resultList.add(customerSummary);
        }

        return resultList;
    }*/

}
