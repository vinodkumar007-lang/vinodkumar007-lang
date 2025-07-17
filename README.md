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

    // 🔁 Group by accountNumber + customerId
    Map<String, SummaryProcessedFile> groupedMap = new LinkedHashMap<>();

    for (SummaryProcessedFile spf : customerList) {
        String account = spf.getAccountNumber();
        String customer = spf.getCustomerId();
        if (account == null || account.isBlank()) continue;

        String key = account + "::" + customer;
        SummaryProcessedFile existing = groupedMap.computeIfAbsent(key, k -> {
            SummaryProcessedFile newSpf = new SummaryProcessedFile();
            BeanUtils.copyProperties(spf, newSpf);
            return newSpf;
        });

        for (String folder : folders) {
            Path folderPath = jobDir.resolve(folder);
            Optional<Path> fileOpt;

            if (folder.equals("mobstat")) {
                fileOpt = Files.exists(folderPath)
                        ? Files.list(folderPath)
                        .filter(p -> p.getFileName().toString().toLowerCase().contains("mobstat_trigger") &&
                                p.getFileName().toString().contains(account))
                        .findFirst()
                        : Optional.empty();
            } else {
                fileOpt = Files.exists(folderPath)
                        ? Files.list(folderPath)
                        .filter(p -> p.getFileName().toString().contains(account))
                        .findFirst()
                        : Optional.empty();
            }

            String outputMethod = folderToOutputMethod.get(folder);
            Map<String, String> errorEntry = errorMap.getOrDefault(account, Collections.emptyMap());
            String failureStatus = errorEntry.getOrDefault(outputMethod, "");

            if (fileOpt.isPresent()) {
                Path file = fileOpt.get();
                String blobUrl = blobStorageService.uploadFile(
                        file.toFile(),
                        msg.getSourceSystem() + "/" + msg.getBatchId() + "/" + folder + "/" + file.getFileName()
                );
                String decoded = decodeUrl(blobUrl);

                switch (folder) {
                    case "email" -> {
                        existing.setPdfEmailFileUrl(decoded);
                        existing.setPdfEmailStatus("OK");
                    }
                    case "archive" -> {
                        existing.setPdfArchiveFileUrl(decoded);
                        existing.setPdfArchiveStatus("OK");
                    }
                    case "mobstat" -> {
                        existing.setPdfMobstatFileUrl(decoded);
                        existing.setPdfMobstatStatus("OK");
                    }
                    case "print" -> {
                        existing.setPrintFileUrl(decoded);
                        existing.setPrintStatus("OK");
                    }
                }
            } else {
                boolean isExplicitFail = "Failed".equalsIgnoreCase(failureStatus);
                switch (folder) {
                    case "email" -> existing.setPdfEmailStatus(isExplicitFail ? "Failed" : "");
                    case "archive" -> existing.setPdfArchiveStatus(isExplicitFail ? "Failed" : "");
                    case "mobstat" -> existing.setPdfMobstatStatus(isExplicitFail ? "Failed" : "");
                    case "print" -> existing.setPrintStatus(isExplicitFail ? "Failed" : "");
                }
            }
        }

        // 🛑 Skip customers with no delivery
        List<String> statuses = Arrays.asList(
                existing.getPdfEmailStatus(),
                existing.getPdfArchiveStatus(),
                existing.getPdfMobstatStatus(),
                existing.getPrintStatus()
        );
        boolean noDelivery = statuses.stream().allMatch(s -> s == null || s.isBlank());
        if (noDelivery) {
            groupedMap.remove(key); // remove from result
            continue;
        }

        // ✅ Final status
        long failed = statuses.stream().filter("Failed"::equalsIgnoreCase).count();
        long success = statuses.stream().filter("OK"::equalsIgnoreCase).count();

        if (failed > 0 && success == 0) {
            existing.setStatusCode("FAILED");
            existing.setStatusDescription("All methods failed");
        } else if (failed > 0) {
            existing.setStatusCode("PARTIAL");
            existing.setStatusDescription("Some methods failed");
        } else {
            existing.setStatusCode("SUCCESS");
            existing.setStatusDescription("Success");
        }
    }

    return new ArrayList<>(groupedMap.values());
}
==========================
package com.nedbank.kafka.filemanage.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import lombok.Data;

import java.util.List;

@Data
@JsonInclude(JsonInclude.Include.NON_NULL)
public class SummaryPayload {
    private String batchID;
    private String fileName;
    private Header header;
    private Metadata metadata;
    private Payload payload;
    private List<SummaryProcessedFile> processedFiles;
    private List<PrintFile> printFiles;
    private String mobstatTriggerFile;

    @JsonIgnore
    private String summaryFileURL; // Will not be written to summary.json

    private String fileLocation;
    private String timestamp;
}
========================
 public String uploadFile(File file, String targetPath) {
        try {
            initSecrets();
            BlobServiceClient blobClient = new BlobServiceClientBuilder()
                    .endpoint(String.format(azureStorageFormat, accountName))
                    .credential(new StorageSharedKeyCredential(accountName, accountKey))
                    .buildClient();

            BlobClient blob = blobClient.getBlobContainerClient(containerName).getBlobClient(targetPath);
            try (InputStream inputStream = new FileInputStream(file)) {
                blob.upload(inputStream, file.length(), true);
            }

            logger.info("Uploaded binary file to '{}'", blob.getBlobUrl());
            return blob.getBlobUrl();
        } catch (Exception e) {
            logger.error("Upload failed for binary file: {}", e.getMessage(), e);
            throw new CustomAppException("Binary upload failed", 605, HttpStatus.INTERNAL_SERVER_ERROR, e);
        }
    }
