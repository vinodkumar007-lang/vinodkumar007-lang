2025-07-21T21:10:45.186+02:00 ERROR 1 --- [pool-1-thread-1] c.n.k.f.service.KafkaListenerService     : ❌ Error post-OT summary generation
java.lang.NullPointerException: Cannot invoke "String.toUpperCase()" because "method" is null
 at com.nedbank.kafka.filemanage.utils.SummaryJsonWriter.buildProcessedFileEntries(SummaryJsonWriter.java:157) ~[classes!/:na]
 at com.nedbank.kafka.filemanage.utils.SummaryJsonWriter.buildPayload(SummaryJsonWriter.java:76) ~[classes!/:na]
 at com.nedbank.kafka.filemanage.service.KafkaListenerService.processAfterOT(KafkaListenerService.java:223) ~[classes!/:na]
 at com.nedbank.kafka.filemanage.service.KafkaListenerService.lambda$onKafkaMessage$0(KafkaListenerService.java:110) ~[classes!/:na]
 at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:539) ~[na:na]
 at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264) ~[na:na]
 at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136) ~[na:na]



 private static List<ProcessedFileEntry> buildProcessedFileEntries(
            List<SummaryProcessedFile> processedList,
            Map<String, Map<String, String>> errorMap) {

        List<ProcessedFileEntry> finalList = new ArrayList<>();

        // Group by customerId::accountNumber
        Map<String, List<SummaryProcessedFile>> grouped = processedList.stream()
                .filter(f -> f.getCustomerId() != null && f.getAccountNumber() != null)
                .collect(Collectors.groupingBy(f -> f.getCustomerId() + "::" + f.getAccountNumber()));

        for (Map.Entry<String, List<SummaryProcessedFile>> entry : grouped.entrySet()) {
            String key = entry.getKey();
            String[] parts = key.split("::");
            String customerId = parts[0];
            String accountNumber = parts[1];
            List<SummaryProcessedFile> list = entry.getValue();

            ProcessedFileEntry pfEntry = new ProcessedFileEntry();
            pfEntry.setCustomerId(customerId);
            pfEntry.setAccountNumber(accountNumber);

            for (SummaryProcessedFile file : list) {
                String method = file.getOutputType();
                String status = file.getStatus();
                String url = file.getBlobURL();

                switch (method.toUpperCase()) {
                    case "EMAIL":
                        pfEntry.setPdfEmailFileUrl(url);
                        pfEntry.setPdfEmailFileUrlStatus(status);
                        break;
                    case "ARCHIVE":
                        pfEntry.setPdfArchiveFileUrl(url);
                        pfEntry.setPdfArchiveFileUrlStatus(status);
                        break;
                    case "MOBSTAT":
                        pfEntry.setPdfMobstatFileUrl(url);
                        pfEntry.setPdfMobstatFileUrlStatus(status);
                        break;
                    case "PRINT":
                        pfEntry.setPrintFileUrl(url);
                        pfEntry.setPrintFileUrlStatus(status);
                        break;
                }
            }

            // Now determine overallStatus using full errorMap
            pfEntry.setOverAllStatusCode(determineOverallStatus(pfEntry, errorMap.getOrDefault(key, new HashMap<>())));
            finalList.add(pfEntry);
        }

        return finalList;
    }

    private static String determineOverallStatus(ProcessedFileEntry pf, Map<String, String> methodErrors) {
        List<String> methods = new ArrayList<>();

        if (pf.getPdfEmailFileUrlStatus() != null) {
            methods.add("EMAIL");
            methods.add("ARCHIVE");
        } else if (pf.getPdfMobstatFileUrlStatus() != null) {
            methods.add("MOBSTAT");
            methods.add("ARCHIVE");
        } else if (pf.getPrintFileUrlStatus() != null) {
            methods.add("PRINT");
            methods.add("ARCHIVE");
        }

        int successCount = 0;
        int failedInErrorMapCount = 0;
        int failedNotInErrorMapCount = 0;

        for (String method : methods) {
            String status = getStatusByMethod(pf, method);
            if ("SUCCESS".equalsIgnoreCase(status)) {
                successCount++;
            } else if ("FAILED".equalsIgnoreCase(status)) {
                if (methodErrors.containsKey(method)) {
                    failedInErrorMapCount++;
                } else {
                    failedNotInErrorMapCount++;
                }
            }
        }

        if (successCount == methods.size()) {
            return "SUCCESS";
        } else if (failedInErrorMapCount > 0) {
            return "FAILED";
        } else {
            return "PARTIAL";
        }
    }
    private static String getStatusByMethod(ProcessedFileEntry pf, String method) {
        switch (method.toUpperCase()) {
            case "EMAIL":
                return pf.getPdfEmailFileUrlStatus();
            case "ARCHIVE":
                return pf.getPdfArchiveFileUrlStatus();
            case "MOBSTAT":
                return pf.getPdfMobstatFileUrlStatus();
            case "PRINT":
                return pf.getPrintFileUrlStatus();
            default:
                return null;
        }
    }
