private List<SummaryProcessedFile> buildDetailedProcessedFiles(
            Path jobDir,
            List<SummaryProcessedFile> customerList,
            Map<String, Map<String, String>> errorMap,
            KafkaMessage msg) throws IOException {

        List<String> folders = List.of("email", "archive", "mobstat", "print");
        Map<String, String> folderToOutputMethod = Map.of(
                "email", "EMAIL",
                "archive", "ARCHIVE",
                "mobstat", "MOBSTAT",
                "print", "PRINT"
        );

        List<SummaryProcessedFile> finalList = new ArrayList<>();

        for (SummaryProcessedFile customer : customerList) {
            String account = customer.getAccountNumber();
            String customerId = customer.getCustomerId();
            Map<String, Boolean> methodAdded = new HashMap<>();

            // Step 1: check all folders and add successful or errorMap-driven entries
            for (String folder : folders) {
                methodAdded.put(folder, false);
                Path folderPath = jobDir.resolve(folder);
                if (!Files.exists(folderPath)) continue;

                Optional<Path> match = Files.list(folderPath)
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().contains(account))
                        .findFirst();

                String method = folderToOutputMethod.get(folder);

                if (match.isPresent()) {
                    Path filePath = match.get();
                    File file = filePath.toFile();
                    String blobUrl = blobStorageService.uploadFileByMessage(file, folder, msg);

                    SummaryProcessedFile entry = new SummaryProcessedFile();
                    BeanUtils.copyProperties(customer, entry);
                    entry.setOutputMethod(method);
                    entry.setBlobURL(blobUrl);

                    if (errorMap.containsKey(account) && errorMap.get(account).containsKey(method)) {
                        String status = errorMap.get(account).get(method);
                        if ("FAILED".equalsIgnoreCase(status)) {
                            entry.setStatus("FAILED");
                            entry.setStatusDescription("Marked as FAILED from ErrorReport");
                        } else {
                            entry.setStatus("PARTIAL");
                            entry.setStatusDescription("Marked as PARTIAL from ErrorReport");
                        }
                    } else {
                        entry.setStatus("SUCCESS");
                        entry.setStatusDescription("No error found in ErrorReport");
                    }

                    if ("archive".equals(folder)) {
                        for (String deliveryFolder : List.of("email", "mobstat", "print")) {
                            Path deliveryPath = jobDir.resolve(deliveryFolder);
                            if (Files.exists(deliveryPath)) {
                                boolean found = Files.list(deliveryPath)
                                        .filter(Files::isRegularFile)
                                        .anyMatch(p -> p.getFileName().toString().contains(account));
                                if (found) {
                                    entry.setLinkedDeliveryType(deliveryFolder.toUpperCase());
                                    break;
                                }
                            }
                        }
                    }

                    finalList.add(entry);
                    methodAdded.put(folder, true);
                }
            }

            // Step 2: Add entries from errorMap where files were not found
            if (errorMap.containsKey(account)) {
                Map<String, String> methodErrors = errorMap.get(account);

                for (String folder : folders) {
                    if (methodAdded.getOrDefault(folder, false)) continue;

                    String method = folderToOutputMethod.get(folder);
                    if (!methodErrors.containsKey(method)) continue;

                    Path folderPath = jobDir.resolve(folder);
                    if (!Files.exists(folderPath)) continue;

                    SummaryProcessedFile entry = new SummaryProcessedFile();
                    BeanUtils.copyProperties(customer, entry);
                    entry.setOutputMethod(method);
                    entry.setBlobURL(null);

                    String status = methodErrors.get(method);
                    if ("FAILED".equalsIgnoreCase(status)) {
                        entry.setStatus("FAILED");
                        entry.setStatusDescription("Marked as FAILED from ErrorReport (file not found)");
                    } else {
                        entry.setStatus("PARTIAL");
                        entry.setStatusDescription("Marked as PARTIAL from ErrorReport (file not found)");
                    }

                    if ("archive".equals(folder)) {
                        for (String deliveryFolder : List.of("email", "mobstat", "print")) {
                            Path deliveryPath = jobDir.resolve(deliveryFolder);
                            if (Files.exists(deliveryPath)) {
                                boolean found = Files.list(deliveryPath)
                                        .filter(Files::isRegularFile)
                                        .anyMatch(p -> p.getFileName().toString().contains(account));
                                if (found) {
                                    entry.setLinkedDeliveryType(deliveryFolder.toUpperCase());
                                    break;
                                }
                            }
                        }
                    }

                    finalList.add(entry);
                    methodAdded.put(folder, true);
                }
            }

            // Step 3: Add default NOT_FOUND for remaining methods
            for (String folder : folders) {
                if (methodAdded.getOrDefault(folder, false)) continue;

                String method = folderToOutputMethod.get(folder);

                SummaryProcessedFile entry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, entry);
                entry.setOutputMethod(method);
                entry.setBlobURL(null);
                entry.setStatus("NOT_FOUND");
                entry.setStatusDescription("No file found and not listed in ErrorReport");

                if ("archive".equals(folder)) {
                    for (String deliveryFolder : List.of("email", "mobstat", "print")) {
                        Path deliveryPath = jobDir.resolve(deliveryFolder);
                        if (Files.exists(deliveryPath)) {
                            boolean found = Files.list(deliveryPath)
                                    .filter(Files::isRegularFile)
                                    .anyMatch(p -> p.getFileName().toString().contains(account));
                            if (found) {
                                entry.setLinkedDeliveryType(deliveryFolder.toUpperCase());
                                break;
                            }
                        }
                    }
                }

                finalList.add(entry);
            }
        }

        return finalList;
    }



    ======================

    package com.nedbank.kafka.filemanage.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.nedbank.kafka.filemanage.model.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

import java.io.File;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Collectors;

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

    public static SummaryPayload buildPayload(
            KafkaMessage kafkaMessage,
            List<SummaryProcessedFile> processedList,
            String summaryBlobUrl,
            String fileName,
            String batchId,
            String timestamp,
            Map<String, Map<String, String>> errorMap
    ) {
        SummaryPayload payload = new SummaryPayload();
        payload.setBatchID(batchId);
        payload.setFileName(fileName);
        payload.setTimestamp(timestamp);
        payload.setSummaryFileURL(summaryBlobUrl);

        Header header = new Header();
        header.setTenantCode(kafkaMessage.getTenantCode());
        header.setChannelID(kafkaMessage.getChannelID());
        header.setAudienceID(kafkaMessage.getAudienceID());
        header.setTimestamp(timestamp);
        header.setSourceSystem(kafkaMessage.getSourceSystem());
        header.setProduct(kafkaMessage.getSourceSystem());
        header.setJobName(kafkaMessage.getSourceSystem());
        payload.setHeader(header);

        List<ProcessedFileEntry> processedFileEntries = buildProcessedFileEntries(processedList, errorMap);
        payload.setProcessedFileList(processedFileEntries);

        int totalFileUrls = processedFileEntries.size();

        Payload payloadInfo = new Payload();
        payloadInfo.setUniqueECPBatchRef(kafkaMessage.getUniqueECPBatchRef());
        payloadInfo.setRunPriority(kafkaMessage.getRunPriority());
        payloadInfo.setEventID(kafkaMessage.getEventID());
        payloadInfo.setEventType(kafkaMessage.getEventType());
        payloadInfo.setRestartKey(kafkaMessage.getRestartKey());
        payloadInfo.setFileCount(totalFileUrls);
        payload.setPayload(payloadInfo);

        Metadata metadata = new Metadata();
        metadata.setTotalCustomersProcessed((int) processedFileEntries.stream()
                .map(pf -> pf.getCustomerId() + "::" + pf.getAccountNumber())
                .distinct()
                .count());

        long total = processedFileEntries.size();
        long success = processedFileEntries.stream()
                .filter(entry -> "SUCCESS".equalsIgnoreCase(entry.getOverallStatus()))
                .count();
        long failed = processedFileEntries.stream()
                .filter(entry -> "FAILED".equalsIgnoreCase(entry.getOverallStatus()))
                .count();

        String overallStatus;
        if (success == total) {
            overallStatus = "SUCCESS";
        } else if (failed == total) {
            overallStatus = "FAILED";
        } else {
            overallStatus = "PARTIAL";
        }

        metadata.setProcessingStatus(overallStatus);
        metadata.setEventOutcomeCode("0");
        metadata.setEventOutcomeDescription(overallStatus.toLowerCase());
        payload.setMetadata(metadata);

        return payload;
    }

    private static List<ProcessedFileEntry> buildProcessedFileEntries(
            List<SummaryProcessedFile> processedList,
            Map<String, Map<String, String>> errorMap // key = customerId::accountNumber, value = Map<outputMethod, reason>
    ) {
        List<ProcessedFileEntry> finalList = new ArrayList<>();

        if (processedList == null || processedList.isEmpty()) {
            return finalList;
        }

        // Group by customerId::accountNumber
        Map<String, List<SummaryProcessedFile>> grouped = processedList.stream()
                .filter(f -> f.getCustomerId() != null && f.getAccountNumber() != null)
                .collect(Collectors.groupingBy(f -> f.getCustomerId() + "::" + f.getAccountNumber()));

        for (Map.Entry<String, List<SummaryProcessedFile>> entry : grouped.entrySet()) {
            String[] parts = entry.getKey().split("::");
            String customerId = parts[0];
            String accountNumber = parts[1];
            List<SummaryProcessedFile> records = entry.getValue();

            // Map of outputMethod -> SummaryProcessedFile
            Map<String, SummaryProcessedFile> typeMap = records.stream()
                    .collect(Collectors.toMap(SummaryProcessedFile::getOutputMethod, f -> f, (a, b) -> a));

            SummaryProcessedFile archive = typeMap.get("ARCHIVE");
            if (archive == null) continue; // Archive is mandatory

            // Collect requested outputMethods from errorMap and typeMap keys
            Set<String> requestedOutputs = new HashSet<>(errorMap.getOrDefault(entry.getKey(), Collections.emptyMap()).keySet());
            requestedOutputs.addAll(typeMap.keySet());
            requestedOutputs.remove("ARCHIVE"); // Exclude archive itself

            for (String outputMethod : requestedOutputs) {
                ProcessedFileEntry entryObj = new ProcessedFileEntry();
                entryObj.setCustomerId(customerId);
                entryObj.setAccountNumber(accountNumber);
                entryObj.setOutputMethod(outputMethod);

                // Archive info
                entryObj.setArchiveBlobUrl(archive.getBlobURL());
                entryObj.setArchiveStatus(archive.getStatus());

                SummaryProcessedFile output = typeMap.get(outputMethod);
                if (output != null) {
                    entryObj.setOutputBlobUrl(output.getBlobURL());
                    entryObj.setOutputStatus(output.getStatus());
                } else {
                    Map<String, String> failedMap = errorMap.getOrDefault(entry.getKey(), Collections.emptyMap());
                    entryObj.setOutputBlobUrl(null);

                    if (failedMap.containsKey(outputMethod)) {
                        entryObj.setOutputStatus("FAILED");
                    } else {
                        entryObj.setOutputStatus("NOT_FOUND");
                    }
                }

                // Compute overallStatus for this outputMethod + archive pair
                String archiveStatus = entryObj.getArchiveStatus();
                String outputStatus = entryObj.getOutputStatus();

                if ("SUCCESS".equalsIgnoreCase(outputStatus) && "SUCCESS".equalsIgnoreCase(archiveStatus)) {
                    entryObj.setOverallStatus("SUCCESS");
                } else if ("FAILED".equalsIgnoreCase(outputStatus) || "FAILED".equalsIgnoreCase(archiveStatus)) {
                    entryObj.setOverallStatus("FAILED");
                } else {
                    entryObj.setOverallStatus("PARTIAL");
                }

                finalList.add(entryObj);
            }
        }

        return finalList;
    }
}
