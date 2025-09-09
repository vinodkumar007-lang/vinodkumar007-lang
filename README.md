private String findFileByAccount(Map<String, String> fileMap, String account) {
    if (fileMap == null || fileMap.isEmpty() || account == null) return null;

    return fileMap.entrySet().stream()
            .filter(e -> {
                String fileName = e.getKey();
                return fileName.contains(account); // match account anywhere in filename
            })
            .map(Map.Entry::getValue)
            .findFirst()
            .orElse(null);
}

for (SummaryProcessedFile customer : customerList) {
    if (customer == null || customer.getAccountNumber() == null) continue;

    String account = customer.getAccountNumber();
    Map<String, String> archivesForAccount = accountToArchiveMap.getOrDefault(account, Collections.emptyMap());

    // if no archive, still create entry
    if (archivesForAccount.isEmpty()) {
        SummaryProcessedFile entry = new SummaryProcessedFile();
        BeanUtils.copyProperties(customer, entry);

        entry.setPdfEmailFileUrl(findFileByAccount(deliveryFileMaps.get(AppConstants.FOLDER_EMAIL), account));
        entry.setPdfMobstatFileUrl(findFileByAccount(deliveryFileMaps.get(AppConstants.FOLDER_MOBSTAT), account));
        entry.setPrintFileUrl(findFileByAccount(deliveryFileMaps.get(AppConstants.FOLDER_PRINT), account));

        finalList.add(entry);
    } else {
        for (Map.Entry<String, String> archiveEntry : archivesForAccount.entrySet()) {
            String archiveFileName = archiveEntry.getKey();
            String archiveUrl = archiveEntry.getValue();

            SummaryProcessedFile entry = new SummaryProcessedFile();
            BeanUtils.copyProperties(customer, entry);
            entry.setArchiveBlobUrl(archiveUrl);

            entry.setPdfEmailFileUrl(findFileByAccount(deliveryFileMaps.get(AppConstants.FOLDER_EMAIL), account));
            entry.setPdfMobstatFileUrl(findFileByAccount(deliveryFileMaps.get(AppConstants.FOLDER_MOBSTAT), account));
            entry.setPrintFileUrl(findFileByAccount(deliveryFileMaps.get(AppConstants.FOLDER_PRINT), account));

            finalList.add(entry);
        }
    }
}
