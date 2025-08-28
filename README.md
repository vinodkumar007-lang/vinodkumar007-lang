    private List<SummaryProcessedFile> buildDetailedProcessedFiles(
            Path jobDir,
            List<SummaryProcessedFile> customerList,
            Map<String, Map<String, String>> errorMap,
            KafkaMessage msg) throws IOException {

        List<SummaryProcessedFile> finalList = new ArrayList<>();

        if (jobDir == null) {
            logger.warn("[{}] ⚠️ jobDir is null. Skipping buildDetailedProcessedFiles.", msg != null ? msg.getBatchId() : "UNKNOWN");
            return finalList;
        }
        if (customerList == null || customerList.isEmpty()) {
            logger.warn("[{}] ⚠️ customerList is null/empty. Nothing to process.", msg != null ? msg.getBatchId() : "UNKNOWN");
            return finalList;
        }
        if (msg == null) {
            logger.warn("[UNKNOWN] ⚠️ KafkaMessage is null. Skipping buildDetailedProcessedFiles.");
            return finalList;
        }

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

        // -------- Upload all archive files --------
        Path archivePath = jobDir.resolve(AppConstants.FOLDER_ARCHIVE);
        Map<String, Map<String, String>> accountToArchiveMap = new HashMap<>();

        if (Files.exists(archivePath)) {
            try (Stream<Path> stream = Files.walk(archivePath)) {
                stream.filter(Files::isRegularFile).forEach(file -> {
                    if (!Files.exists(file)) {
                        logger.warn("[{}] ⏩ Skipping missing archive file: {}", msg.getBatchId(), file);
                        return;
                    }

                    String fileName = file.getFileName().toString();
                    String account = extractAccountFromFileName(fileName);
                    if (account == null) {
                        logger.debug("[{}] ⚠️ Skipping archive file without account mapping: {}", msg.getBatchId(), fileName);
                        return;
                    }

                    try {
                        String archiveUrl = decodeUrl(
                                blobStorageService.uploadFileByMessage(file.toFile(), AppConstants.FOLDER_ARCHIVE, msg)
                        );
                        accountToArchiveMap
                                .computeIfAbsent(account, k -> new HashMap<>())
                                .put(fileName, archiveUrl);

                        logger.info("[{}] 📦 Uploaded archive file for account [{}]: {}", msg.getBatchId(), account, archiveUrl);
                    } catch (Exception e) {
                        logger.error("[{}] ⚠️ Failed to upload archive file {}: {}", msg.getBatchId(), fileName, e.getMessage(), e);
                        errorMap.computeIfAbsent(account, k -> new HashMap<>())
                                .put(fileName, "Archive upload failed: " + e.getMessage());
                    }
                });
            }
        } else {
            logger.warn("[{}] ⚠️ Archive folder does not exist: {}", msg.getBatchId(), archivePath);
        }

        // -------- Upload delivery files --------
        Map<String, String> emailFileMap = new HashMap<>();
        Map<String, String> mobstatFileMap = new HashMap<>();
        Map<String, String> printFileMap = new HashMap<>();

        for (String folder : deliveryFolders) {
            Path folderPath = jobDir.resolve(folder);
            if (!Files.exists(folderPath)) {
                logger.debug("[{}] ℹ️ Delivery folder not found: {}", msg.getBatchId(), folder);
                continue;
            }

            try (Stream<Path> stream = Files.walk(folderPath)) {
                stream.filter(Files::isRegularFile).forEach(file -> {
                    if (!Files.exists(file)) {
                        logger.warn("[{}] ⏩ Skipping missing {} file: {}", msg.getBatchId(), folder, file);
                        return;
                    }

                    String fileName = file.getFileName().toString();
                    try {
                        String url = decodeUrl(
                                blobStorageService.uploadFileByMessage(file.toFile(), folder, msg)
                        );
                        switch (folder) {
                            case AppConstants.FOLDER_EMAIL -> emailFileMap.put(fileName, url);
                            case AppConstants.FOLDER_MOBSTAT -> mobstatFileMap.put(fileName, url);
                            case AppConstants.FOLDER_PRINT -> printFileMap.put(fileName, url);
                        }
                        logger.info("[{}] ✅ Uploaded {} file: {}", msg.getBatchId(), folderToOutputMethod.get(folder), url);
                    } catch (Exception e) {
                        logger.error("[{}] ⚠️ Failed to upload {} file {}: {}", msg.getBatchId(),
                                folderToOutputMethod.getOrDefault(folder, folder), fileName, e.getMessage(), e);
                        errorMap.computeIfAbsent("UNKNOWN", k -> new HashMap<>())
                                .put(fileName, folderToOutputMethod.getOrDefault(folder, folder) + " upload failed: " + e.getMessage());
                    }
                });
            }
        }

        // -------- Build final list --------
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
                    entry.setPdfEmailFileUrl(findFileByAccount(emailFileMap, account));
                    entry.setPdfMobstatFileUrl(findFileByAccount(mobstatFileMap, account));
                    entry.setPrintFileUrl(findFileByAccount(printFileMap, account));
                } else {
                    entry.setPdfEmailFileUrl(emailFileMap.get(archiveFileName));
                    entry.setPdfMobstatFileUrl(mobstatFileMap.get(archiveFileName));
                    entry.setPrintFileUrl(printFileMap.get(archiveFileName));
                }

                finalList.add(entry);
            }
        }


        025-08-28T12:47:33.262+02:00  WARN 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : [81d7dfb9-cb41-4a47-8438-8e686b0aec52] ⚠️ Archive folder does not exist: /mnt/nfs/ete-exstream/ete-SA/output/MFC/0306caa0-8cda-4b51-8620-a1ee390be956/archive
2025-08-28T12:47:33.304+02:00 ERROR 1 --- [pool-1-thread-2] c.n.k.f.service.BlobStorageService       : ❌ Error reading file for upload: /mnt/nfs/ete-exstream/ete-SA/output/MFC/0306caa0-8cda-4b51-8620-a1ee390be956/email/8aec451a-f075-49aa-8e3c-47d3159bb02c.tmp

java.nio.file.NoSuchFileException: /mnt/nfs/ete-exstream/ete-SA/output/MFC/0306caa0-8cda-4b51-8620-a1ee390be956/email/8aec451a-f075-49aa-8e3c-47d3159bb02c.tmp
	at java.base/sun.nio.fs.UnixException.translateToIOException(UnixException.java:92) ~[na:na]
	at java.base/sun.nio.fs.UnixException.rethrowAsIOException(UnixException.java:106) ~[na:na]
	at java.base/sun.nio.fs.UnixException.rethrowAsIOException(UnixException.java:111) ~[na:na]
	at java.base/sun.nio.fs.UnixFileSystemProvider.newByteChannel(UnixFileSystemProvider.java:218) ~[na:na]
	at java.base/java.nio.file.Files.newByteChannel(Files.java:380) ~[na:na]
	at java.base/java.nio.file.Files.newByteChannel(Files.java:432) ~[na:na]
	at java.base/java.nio.file.Files.readAllBytes(Files.java:3288) ~[na:na]
	at com.nedbank.kafka.filemanage.service.BlobStorageService.uploadFileByMessage(BlobStorageService.java:221) ~[classes!/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.lambda$buildDetailedProcessedFiles$17(KafkaListenerService.java:710) ~[classes!/:na]
	at java.base/java.util.stream.ForEachOps$ForEachOp$OfRef.accept(ForEachOps.java:183) ~[na:na]
	at java.base/java.util.stream.ReferencePipeline$2$1.accept(ReferencePipeline.java:179) ~[na:na]
	at java.base/java.util.stream.ReferencePipeline$3$1.accept(ReferencePipeline.java:197) ~[na:na]
	at java.base/java.util.Iterator.forEachRemaining(Iterator.java:133) ~[na:na]
	at java.base/java.util.Spliterators$IteratorSpliterator.forEachRemaining(Spliterators.java:1845) ~[na:na]
	at java.base/java.util.stream.AbstractPipeline.copyInto(AbstractPipeline.java:509) ~[na:na]
	at java.base/java.util.stream.AbstractPipeline.wrapAndCopyInto(AbstractPipeline.java:499) ~[na:na]
	at java.base/java.util.stream.ForEachOps$ForEachOp.evaluateSequential(ForEachOps.java:150) ~[na:na]
	at java.base/java.util.stream.ForEachOps$ForEachOp$OfRef.evaluateSequential(ForEachOps.java:173) ~[na:na]
	at java.base/java.util.stream.AbstractPipeline.evaluate(AbstractPipeline.java:234) ~[na:na]
	at java.base/java.util.stream.ReferencePipeline.forEach(ReferencePipeline.java:596) ~[na:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.buildDetailedProcessedFiles(KafkaListenerService.java:701) ~[classes!/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.processAfterOT(KafkaListenerService.java:328) ~[classes!/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.lambda$onKafkaMessage$2(KafkaListenerService.java:196) ~[classes!/:na]
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:539) ~[na:na]
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264) ~[na:na]
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136) ~[na:na]
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635) ~[na:na]
	at java.base/java.lang.Thread.run(Thread.java:840) ~[na:na]

2025-08-28T12:47:33.360+02:00 ERROR 1 --- [pool-1-thread-2] c.n.k.f.service.KafkaListenerService     : [81d7dfb9-cb41-4a47-8438-8e686b0aec52] ⚠️ Failed to upload EMAIL file 8aec451a-f075-49aa-8e3c-47d3159bb02c.tmp: File read failed

com.nedbank.kafka.filemanage.exception.CustomAppException: File read failed
	at com.nedbank.kafka.filemanage.service.BlobStorageService.uploadFileByMessage(BlobStorageService.java:226) ~[classes!/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.lambda$buildDetailedProcessedFiles$17(KafkaListenerService.java:710) ~[classes!/:na]
	at java.base/java.util.stream.ForEachOps$ForEachOp$OfRef.accept(ForEachOps.java:183) ~[na:na]
	at java.base/java.util.stream.ReferencePipeline$2$1.accept(ReferencePipeline.java:179) ~[na:na]
	at java.base/java.util.stream.ReferencePipeline$3$1.accept(ReferencePipeline.java:197) ~[na:na]
	at java.base/java.util.Iterator.forEachRemaining(Iterator.java:133) ~[na:na]
	at java.base/java.util.Spliterators$IteratorSpliterator.forEachRemaining(Spliterators.java:1845) ~[na:na]
	at java.base/java.util.stream.AbstractPipeline.copyInto(AbstractPipeline.java:509) ~[na:na]
	at java.base/java.util.stream.AbstractPipeline.wrapAndCopyInto(AbstractPipeline.java:499) ~[na:na]
	at java.base/java.util.stream.ForEachOps$ForEachOp.evaluateSequential(ForEachOps.java:150) ~[na:na]
	at java.base/java.util.stream.ForEachOps$ForEachOp$OfRef.evaluateSequential(ForEachOps.java:173) ~[na:na]
	at java.base/java.util.stream.AbstractPipeline.evaluate(AbstractPipeline.java:234) ~[na:na]
	at java.base/java.util.stream.ReferencePipeline.forEach(ReferencePipeline.java:596) ~[na:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.buildDetailedProcessedFiles(KafkaListenerService.java:701) ~[classes!/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.processAfterOT(KafkaListenerService.java:328) ~[classes!/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.lambda$onKafkaMessage$2(KafkaListenerService.java:196) ~[classes!/:na]
	at java.base/java.util.concurrent.Executors$RunnableAdapter.call(Executors.java:539) ~[na:na]
	at java.base/java.util.concurrent.FutureTask.run(FutureTask.java:264) ~[na:na]
	at java.base/java.util.concurrent.ThreadPoolExecutor.runWorker(ThreadPoolExecutor.java:1136) ~[na:na]
	at java.base/java.util.concurrent.ThreadPoolExecutor$Worker.run(ThreadPoolExecutor.java:635) ~[na:na]
	at java.base/java.lang.Thread.run(Thread.java:840) ~[na:na]
Caused by: java.nio.file.NoSuchFileException: /mnt/nfs/ete-exstream/ete-SA/output/MFC/0306caa0-8cda-4b51-8620-a1ee390be956/email/8aec451a-f075-49aa-8e3c-47d3159bb02c.tmp
	at java.base/sun.nio.fs.UnixException.translateToIOException(UnixException.java:92) ~[na:na]
	at java.base/sun.nio.fs.UnixException.rethrowAsIOException(UnixException.java:106) ~[na:na]
	at java.base/sun.nio.fs.UnixException.rethrowAsIOException(UnixException.java:111) ~[na:na]
	at java.base/sun.nio.fs.UnixFileSystemProvider.newByteChannel(UnixFileSystemProvider.java:218) ~[na:na]
	at java.base/java.nio.file.Files.newByteChannel(Files.java:380) ~[na:na]
	at java.base/java.nio.file.Files.newByteChannel(Files.java:432) ~[na:na]
	at java.base/java.nio.file.Files.readAllBytes(Files.java:3288) ~[na:na]
	at com.nedbank.kafka.filemanage.service.BlobStorageService.uploadFileByMessage(BlobStorageService.java:221) ~[classes!/:na]

        logger.info("[{}] ✅ buildDetailedProcessedFiles completed. Final processed list size={}", msg.getBatchId(), finalList.size());
        return finalList;
    }
