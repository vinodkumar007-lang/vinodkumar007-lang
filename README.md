private List<SummaryProcessedFile> buildFinalProcessedList(
            List<SummaryProcessedFile> customerList,
            Map<String, Map<String, String>> accountToArchiveMap,
            Map<String, Map<String, String>> deliveryFileMaps,
            KafkaMessage msg) {

        List<SummaryProcessedFile> finalList = new ArrayList<>();
        Set<String> uniqueKeys = new HashSet<>();
        boolean isMfc = AppConstants.SOURCE_MFC.equalsIgnoreCase(msg.getSourceSystem());

        for (SummaryProcessedFile customer : customerList) {
            if (customer == null || customer.getAccountNumber() == null) {
                logger.debug("[{}] ⏩ Skipping null/invalid customer entry.", msg.getBatchId());
                continue;
            }

            String account = customer.getAccountNumber();
            Map<String, String> archivesForAccount = accountToArchiveMap.getOrDefault(account, Collections.emptyMap());

            for (Map.Entry<String, String> archiveEntry : archivesForAccount.entrySet()) {
                String archiveFileName = archiveEntry.getKey();
                String archiveUrl = archiveEntry.getValue();

                String key = customer.getCustomerId() + "|" + account + "|" + archiveFileName;
                if (!uniqueKeys.add(key)) {
                    logger.debug("[{}] ⏩ Duplicate entry skipped for key={}", msg.getBatchId(), key);
                    continue;
                }

                SummaryProcessedFile entry = new SummaryProcessedFile();
                BeanUtils.copyProperties(customer, entry);
                entry.setArchiveBlobUrl(archiveUrl);

                if (isMfc) {
                    entry.setPdfEmailFileUrl(findFileByAccount(deliveryFileMaps.get(AppConstants.FOLDER_EMAIL), account));
                    entry.setPdfMobstatFileUrl(findFileByAccount(deliveryFileMaps.get(AppConstants.FOLDER_MOBSTAT), account));
                    entry.setPrintFileUrl(findFileByAccount(deliveryFileMaps.get(AppConstants.FOLDER_PRINT), account));
                } else {
                    entry.setPdfEmailFileUrl(deliveryFileMaps.get(AppConstants.FOLDER_EMAIL).get(archiveFileName));
                    entry.setPdfMobstatFileUrl(deliveryFileMaps.get(AppConstants.FOLDER_MOBSTAT).get(archiveFileName));
                    entry.setPrintFileUrl(deliveryFileMaps.get(AppConstants.FOLDER_PRINT).get(archiveFileName));
                }

                finalList.add(entry);
            }
        }
        return finalList;
    }
