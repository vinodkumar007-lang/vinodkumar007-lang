package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.constants.AppConstants;
import com.nedbank.kafka.filemanage.model.KafkaMessage;
import com.nedbank.kafka.filemanage.model.SummaryProcessedFile;
import com.nedbank.kafka.filemanage.utils.SummaryJsonWriter;
import jakarta.annotation.PreDestroy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.BeanUtils;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.stream.Stream;

@Service
public class KafkaListenerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    private final BlobStorageService blobStorageService;
    private final SummaryJsonWriter summaryJsonWriter;

    public KafkaListenerService(BlobStorageService blobStorageService, SummaryJsonWriter summaryJsonWriter) {
        this.blobStorageService = blobStorageService;
        this.summaryJsonWriter = summaryJsonWriter;
    }

    public List<SummaryProcessedFile> processJob(Path jobDir, List<SummaryProcessedFile> customerList, KafkaMessage msg) throws IOException {
        Map<String, Map<String, String>> errorMap = new HashMap<>();
        return buildDetailedProcessedFiles(jobDir, customerList, errorMap, msg);
    }

    /**
     * Main method to build detailed processed files including archive and delivery files.
     */
    private List<SummaryProcessedFile> buildDetailedProcessedFiles(
            Path jobDir,
            List<SummaryProcessedFile> customerList,
            Map<String, Map<String, String>> errorMap,
            KafkaMessage msg) throws IOException {

        List<SummaryProcessedFile> finalList = new ArrayList<>();

        if (!validateInputs(jobDir, customerList, msg)) return finalList;

        List<String> deliveryFolders = List.of(
                AppConstants.FOLDER_EMAIL,
                AppConstants.FOLDER_MOBSTAT,
                AppConstants.FOLDER_PRINT
        );

        Map<String, String> folderToOutputMethod = Map.of(
                AppConstants.FOLDER_EMAIL, AppConstants.OUTPUT_EMAIL,
                AppConstants.FOLDER_MOBSTAT, AppConstants.OUTPUT_MOBSTAT,
                AppConstants.FOLDER_PRINT, AppConstants.OUTPUT_PRINT
        );

        // Upload all archive files
        Map<String, Map<String, String>> accountToArchiveMap = uploadArchiveFiles(jobDir, msg, errorMap);

        // Upload delivery files
        Map<String, Map<String, String>> deliveryFileMaps = uploadDeliveryFiles(jobDir, deliveryFolders, folderToOutputMethod, msg, errorMap);

        // Build final processed list (one entry per file)
        finalList = buildFinalProcessedList(customerList, accountToArchiveMap, deliveryFileMaps, msg);

        logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
        return finalList;
    }

    private boolean validateInputs(Path jobDir, List<SummaryProcessedFile> customerList, KafkaMessage msg) {
        if (jobDir == null) {
            logger.warn("[{}] ⚠️ jobDir is null. Skipping buildDetailedProcessedFiles.", msg != null ? msg.getBatchId() : "UNKNOWN");
            return false;
        }
        if (customerList == null || customerList.isEmpty()) {
            logger.warn("[{}] ⚠️ customerList is null/empty. Nothing to process.", msg != null ? msg.getBatchId() : "UNKNOWN");
            return false;
        }
        if (msg == null) {
            logger.warn("[UNKNOWN] ⚠️ KafkaMessage is null. Skipping buildDetailedProcessedFiles.");
            return false;
        }
        return true;
    }

    private Map<String, Map<String, String>> uploadArchiveFiles(Path jobDir, KafkaMessage msg, Map<String, Map<String, String>> errorMap) throws IOException {
        Map<String, Map<String, String>> accountToArchiveMap = new HashMap<>();
        Path archivePath = jobDir.resolve(AppConstants.FOLDER_ARCHIVE);

        if (!Files.exists(archivePath)) {
            logger.warn("[{}] ⚠️ Archive folder does not exist: {}", msg.getBatchId(), archivePath);
            return accountToArchiveMap;
        }

        try (Stream<Path> stream = Files.walk(archivePath)) {
            stream.filter(Files::isRegularFile)
                    .filter(file -> !file.getFileName().toString().endsWith(".tmp"))
                    .forEach(file -> processArchiveFile(file, msg, accountToArchiveMap, errorMap));
        }

        return accountToArchiveMap;
    }

    private void processArchiveFile(Path file, KafkaMessage msg,
                                    Map<String, Map<String, String>> accountToArchiveMap,
                                    Map<String, Map<String, String>> errorMap) {
        if (!Files.exists(file)) return;

        String fileName = file.getFileName().toString();
        String account = extractAccountFromFileName(fileName);
        if (account == null) return;

        try {
            String archiveUrl = decodeUrl(blobStorageService.uploadFileByMessage(file.toFile(), AppConstants.FOLDER_ARCHIVE, msg));
            accountToArchiveMap.computeIfAbsent(account, k -> new HashMap<>()).put(fileName, archiveUrl);
            logger.info("[{}] 📦 Uploaded archive file for account [{}]: {}", msg.getBatchId(), account, archiveUrl);
        } catch (Exception e) {
            logger.error("[{}] ⚠️ Failed to upload archive file {}: {}", msg.getBatchId(), fileName, e.getMessage(), e);
            errorMap.computeIfAbsent(account, k -> new HashMap()).put(fileName, "Archive upload failed: " + e.getMessage());
        }
    }

    private Map<String, Map<String, String>> uploadDeliveryFiles(Path jobDir,
                                                                 List<String> deliveryFolders,
                                                                 Map<String, String> folderToOutputMethod,
                                                                 KafkaMessage msg,
                                                                 Map<String, Map<String, String>> errorMap) throws IOException {

        Map<String, Map<String, String>> deliveryFileMaps = new HashMap<>();
        for (String folder : deliveryFolders) deliveryFileMaps.put(folder, new HashMap<>());

        try (Stream<Path> allDirs = Files.walk(jobDir)) {
            List<Path> folderPaths = allDirs
                    .filter(Files::isDirectory)
                    .filter(p -> deliveryFolders.stream()
                            .anyMatch(f -> f.equalsIgnoreCase(p.getFileName().toString())))
                    .toList();

            for (Path folderPath : folderPaths) {
                String folderName = folderPath.getFileName().toString();
                try (Stream<Path> files = Files.walk(folderPath)) {
                    files.filter(Files::isRegularFile)
                            .filter(f -> !f.getFileName().toString().endsWith(".tmp"))
                            .forEach(file -> processDeliveryFile(file, folderName, folderToOutputMethod, msg, deliveryFileMaps, errorMap));
                }
            }
        }

        return deliveryFileMaps;
    }

    private void processDeliveryFile(Path file, String folder,
                                     Map<String, String> folderToOutputMethod,
                                     KafkaMessage msg,
                                     Map<String, Map<String, String>> deliveryFileMaps,
                                     Map<String, Map<String, String>> errorMap) {
        if (!Files.exists(file)) return;

        String fileName = file.getFileName().toString();
        try {
            String url = decodeUrl(blobStorageService.uploadFileByMessage(file.toFile(), folder, msg));
            deliveryFileMaps.get(folder).put(fileName, url);
            logger.info("[{}] ✅ Uploaded {} file: {}", msg.getBatchId(), folder, url);
        } catch (Exception e) {
            errorMap.computeIfAbsent("UNKNOWN", k -> new HashMap<>())
                    .put(fileName, folderToOutputMethod.getOrDefault(folder, folder) + " upload failed: " + e.getMessage());
        }
    }

    /**
     * Build final summary with separate entry per file (archive/email/mobstat/print).
     */
    private List<SummaryProcessedFile> buildFinalProcessedList(List<SummaryProcessedFile> customerList,
                                                               Map<String, Map<String, String>> accountToArchiveMap,
                                                               Map<String, Map<String, String>> deliveryFileMaps,
                                                               KafkaMessage msg) {

        List<SummaryProcessedFile> finalList = new ArrayList<>();

        for (SummaryProcessedFile customer : customerList) {
            if (customer == null || customer.getAccountNumber() == null) continue;
            String account = customer.getAccountNumber();

            Map<String, String> archives = accountToArchiveMap.getOrDefault(account, Collections.emptyMap());
            Map<String, String> emails = deliveryFileMaps.getOrDefault(AppConstants.FOLDER_EMAIL, Collections.emptyMap());
            Map<String, String> mobstats = deliveryFileMaps.getOrDefault(AppConstants.FOLDER_MOBSTAT, Collections.emptyMap());
            Map<String, String> prints = deliveryFileMaps.getOrDefault(AppConstants.FOLDER_PRINT, Collections.emptyMap());

            // Archive entries
            for (Map.Entry<String, String> archiveEntry : archives.entrySet()) {
                SummaryProcessedFile entry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, entry);
                entry.setArchiveBlobUrl(archiveEntry.getValue());
                finalList.add(entry);
            }

            // Email entries
            for (Map.Entry<String, String> emailEntry : emails.entrySet()) {
                if (!emailEntry.getKey().contains(account)) continue;
                SummaryProcessedFile entry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, entry);
                entry.setPdfEmailFileUrl(emailEntry.getValue());
                finalList.add(entry);
            }

            // Mobstat entries
            for (Map.Entry<String, String> mobstatEntry : mobstats.entrySet()) {
                if (!mobstatEntry.getKey().contains(account)) continue;
                SummaryProcessedFile entry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, entry);
                entry.setPdfMobstatFileUrl(mobstatEntry.getValue());
                finalList.add(entry);
            }

            // Print entries
            for (Map.Entry<String, String> printEntry : prints.entrySet()) {
                if (!printEntry.getKey().contains(account)) continue;
                SummaryProcessedFile entry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, entry);
                entry.setPrintFileUrl(printEntry.getValue());
                finalList.add(entry);
            }
        }

        logger.info("[{}] ✅ buildFinalProcessedList completed. Total entries={}", msg.getBatchId(), finalList.size());
        return finalList;
    }

    /**
     * Helper: extract account from file name (supports different suffixes)
     */
    private String extractAccountFromFileName(String fileName) {
        if (fileName == null) return null;
        // Simple example: assumes account is first number group in the file name
        String[] parts = fileName.split("_");
        for (String part : parts) {
            if (part.matches("\\d{10,12}")) return part;
        }
        return null;
    }

    private String decodeUrl(String url) {
        return url; // placeholder for actual decoding logic if needed
    }

    @PreDestroy
    public void cleanup() {
        logger.info("KafkaListenerService shutting down...");
    }
}
