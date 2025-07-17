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
            fileStream.filter(Files::isRegularFile).forEach(file -> {
                String fileName = file.getFileName().toString();
                if (!fileName.contains("_")) return;

                String accountNumber = fileName.split("_")[0];
                String customerId = accountCustomerMap.getOrDefault(accountNumber, "UNKNOWN");
                String key = accountNumber + "_" + customerId;

                SummaryProcessedFile spf = customerFileMap.getOrDefault(key, new SummaryProcessedFile());
                spf.setAccountNumber(accountNumber);
                spf.setCustomerId(customerId);
                spf.getFileUrls().put(folder, msg.getBlobUrl() + "/" + folder + "/" fileName);
                spf.setStatus("SUCCESS");

                customerFileMap.put(key, spf);
            });
        } catch (IOException e) {
            logger.error("❌ Error reading folder {}: {}", folder, e.getMessage());
        }
    }

    // 🛑 Add errorMap-based missing file entries
    for (Map.Entry<String, String> entry : errorMap.entrySet()) {
        String key = entry.getKey(); // accountNumber_deliveryType
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
            spf.getFileUrls().put(deliveryType, "");
            spf.setStatus("FAILED");
        }

        customerFileMap.put(combinedKey, spf);
    }

    finalList.addAll(customerFileMap.values());

    // ➕ Add mobstat_trigger files separately
    Path mobstatTriggerPath = jobDir.resolve("mobstat_trigger");
    if (Files.exists(mobstatTriggerPath)) {
        try (Stream<Path> triggerFiles = Files.list(mobstatTriggerPath)) {
            triggerFiles.filter(Files::isRegularFile).forEach(file -> {
                SummaryProcessedFile trigger = new SummaryProcessedFile();
                trigger.setFileType("mobstat_trigger");
                trigger.setFileURL(msg.getBlobUrl() + "/mobstat_trigger/" + file.getFileName());
                finalList.add(trigger); // Not added to customerFileMap, won't affect count
            });
        } catch (IOException e) {
            logger.error("❌ Error reading mobstat_trigger folder: {}", e.getMessage());
        }
    }

    logger.info("✅ Total unique customer delivery records (excluding trigger): {}", customerFileMap.size());
    return finalList;
}
