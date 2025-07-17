private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        Map<String, String> accountCustomerMap,
        KafkaMessage msg,
        Map<String, String> errorMap
) {
    Map<String, SummaryProcessedFile> customerFileMap = new HashMap<>();
    List<SummaryProcessedFile> finalList = new ArrayList<>();

    List<String> folders = List.of("archive", "email", "mobstat", "print");

    for (String folder : folders) {
        Path folderPath = jobDir.resolve(folder);
        if (!Files.exists(folderPath)) continue;

        try (Stream<Path> fileStream = Files.list(folderPath)) {
            fileStream
                .filter(Files::isRegularFile)
                .forEach(file -> {
                    String fileName = file.getFileName().toString();
                    if (!fileName.contains("_")) return;

                    String accountNumber = fileName.split("_")[0];
                    String customerId = accountCustomerMap.getOrDefault(accountNumber, "UNKNOWN");
                    String key = accountNumber + "_" + customerId;

                    // Initialize or retrieve customer record
                    SummaryProcessedFile spf = customerFileMap.getOrDefault(key, new SummaryProcessedFile());
                    spf.setAccountNumber(accountNumber);
                    spf.setCustomerId(customerId);

                    // Add delivery channel + URL
                    spf.getFileUrls().put(folder, msg.getBlobURL() + "/" + folder + "/" + fileName);

                    // Set status: SUCCESS if file exists
                    spf.setStatus("SUCCESS");

                    customerFileMap.put(key, spf);
                });
        } catch (IOException e) {
            logger.error("❌ Error reading folder {}: {}", folder, e.getMessage());
        }
    }

    // Handle missing files from errorMap
    for (Map.Entry<String, String> entry : errorMap.entrySet()) {
        String key = entry.getKey(); // e.g. accountNumber_deliveryType
        String status = entry.getValue();

        String[] parts = key.split("_");
        if (parts.length != 2) continue;

        String accountNumber = parts[0];
        String deliveryType = parts[1];
        String customerId = accountCustomerMap.getOrDefault(accountNumber, "UNKNOWN");
        String combinedKey = accountNumber + "_" + customerId;

        SummaryProcessedFile spf = customerFileMap.getOrDefault(combinedKey, new SummaryProcessedFile());
        spf.setAccountNumber(accountNumber);
        spf.setCustomerId(customerId);

        if (!spf.getFileUrls().containsKey(deliveryType)) {
            // Only mark as FAILED if file was not already found
            spf.getFileUrls().put(deliveryType, "");
            spf.setStatus("FAILED");
        }

        customerFileMap.put(combinedKey, spf);
    }

    finalList.addAll(customerFileMap.values());

    // ➕ Add mobstat_trigger separately (but not part of count)
    Path mobstatTriggerPath = jobDir.resolve("mobstat_trigger");
    if (Files.exists(mobstatTriggerPath)) {
        try (Stream<Path> triggerFiles = Files.list(mobstatTriggerPath)) {
            triggerFiles
                .filter(Files::isRegularFile)
                .forEach(file -> {
                    SummaryProcessedFile trigger = new SummaryProcessedFile();
                    trigger.setFileType("mobstat_trigger");
                    trigger.setFileURL(msg.getBlobURL() + "/mobstat_trigger/" + file.getFileName());
                    finalList.add(trigger);
                });
        } catch (IOException e) {
            logger.error("❌ Error reading mobstat_trigger: {}", e.getMessage());
        }
    }

    logger.info("📦 Total unique customer delivery records (excluding trigger): {}", customerFileMap.size());
    return finalList;
}

public class SummaryProcessedFile {
    private String accountNumber;
    private String customerId;
    private Map<String, String> fileUrls = new HashMap<>(); // archive, email, mobstat, print
    private String status; // SUCCESS / FAILED / null
    private String fileType; // for trigger file
    private String fileURL;  // for trigger file

    // Getters and setters...
}
