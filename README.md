/**
 * Scans a folder and uploads all files to blob storage.
 * Stores only the filename as the key, and the blob URL as the value.
 */
private Map<String, String> uploadDeliveryFiles(Path folderPath, String folderName, String tenant, String batchId) {
    Map<String, String> fileMap = new HashMap<>();

    if (folderPath == null || !Files.exists(folderPath)) {
        log.warn("Folder {} does not exist for tenant {}", folderName, tenant);
        return fileMap;
    }

    try (Stream<Path> files = Files.list(folderPath)) {
        files.filter(Files::isRegularFile).forEach(file -> {
            try {
                String fileName = file.getFileName().toString(); // ✅ only filename as key
                String blobUrl = blobStorageService.uploadFileAndReturnLocation(
                        file, tenant, batchId, folderName, fileName);

                fileMap.put(fileName, blobUrl); // ✅ key = filename, value = blob URL
                log.info("Uploaded {} file: {} -> {}", folderName, fileName, blobUrl);

            } catch (Exception e) {
                log.error("Failed to upload file {} from folder {}", file, folderName, e);
            }
        });
    } catch (Exception e) {
        log.error("Error scanning folder {} for tenant {}", folderName, tenant, e);
    }

    return fileMap;
}

/**
 * Returns all delivery file URLs matching the account
 */
private List<String> findFilesByAccount(Map<String, String> fileMap, String account) {
    if (fileMap == null || fileMap.isEmpty() || account == null) return Collections.emptyList();

    return fileMap.entrySet().stream()
            .filter(e -> e.getKey().toLowerCase().contains(account.toLowerCase())) // ✅ match inside filename
            .map(Map.Entry::getValue) // ✅ return blob URLs
            .toList();
}

/**
 * Builds processed file entries for all delivery channels (archive, email, print, mobstat).
 */
private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        List<SummaryProcessedFile> customerEntries,
        Map<String, Map<String, String>> deliveryFileMaps) {

    List<SummaryProcessedFile> results = new ArrayList<>();

    for (SummaryProcessedFile entry : customerEntries) {
        String account = entry.getAccountNumber();

        // 🔹 Archive (always expected)
        List<String> archiveFiles = findFilesByAccount(deliveryFileMaps.get(AppConstants.FOLDER_ARCHIVE), account);
        if (!archiveFiles.isEmpty()) {
            for (String url : archiveFiles) {
                SummaryProcessedFile newEntry = new SummaryProcessedFile(entry);
                newEntry.setArchiveBlobUrl(url);
                newEntry.setArchiveStatus("SUCCESS");
                results.add(newEntry);
            }
        }

        // 🔹 Email
        List<String> emailFiles = findFilesByAccount(deliveryFileMaps.get(AppConstants.FOLDER_EMAIL), account);
        if (!emailFiles.isEmpty()) {
            for (String url : emailFiles) {
                SummaryProcessedFile newEntry = new SummaryProcessedFile(entry);
                newEntry.setPdfEmailFileUrl(url);
                newEntry.setEmailStatus("SUCCESS");
                results.add(newEntry);
            }
        }

        // 🔹 Print
        List<String> printFiles = findFilesByAccount(deliveryFileMaps.get(AppConstants.FOLDER_PRINT), account);
        if (!printFiles.isEmpty()) {
            for (String url : printFiles) {
                SummaryProcessedFile newEntry = new SummaryProcessedFile(entry);
                newEntry.setPrintFileUrl(url);
                newEntry.setPrintStatus("SUCCESS");
                results.add(newEntry);
            }
        }

        // 🔹 Mobstat
        List<String> mobstatFiles = findFilesByAccount(deliveryFileMaps.get(AppConstants.FOLDER_MOBSTAT), account);
        if (!mobstatFiles.isEmpty()) {
            for (String url : mobstatFiles) {
                SummaryProcessedFile newEntry = new SummaryProcessedFile(entry);
                newEntry.setPdfMobstatFileUrl(url);
                newEntry.setMobstatStatus("SUCCESS");
                results.add(newEntry);
            }
        }

        // If no files found at all → keep original with failure
        if (archiveFiles.isEmpty() && emailFiles.isEmpty() && printFiles.isEmpty() && mobstatFiles.isEmpty()) {
            SummaryProcessedFile failedEntry = new SummaryProcessedFile(entry);
            failedEntry.setOverallStatus("FAILED");
            results.add(failedEntry);
        }
    }

    return results;
}

