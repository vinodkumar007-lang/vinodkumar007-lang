import static com.nedbank.kafka.filemanage.common.AppConstants.*;

public void onKafkaMessage(String rawMessage, Acknowledgment ack) {
    String batchId = "";
    try {
        logger.info("📩 [batchId: unknown] Received Kafka message: {}", rawMessage);
        KafkaMessage message = objectMapper.readValue(rawMessage, KafkaMessage.class);
        batchId = message.getBatchId();
        List<BatchFile> batchFiles = message.getBatchFiles();
        if (batchFiles == null || batchFiles.isEmpty()) {
            logger.error("❌ [batchId: {}] Rejected - Empty BatchFiles", batchId);
            ack.acknowledge();
            return;
        }

        long dataCount = batchFiles.stream()
                .filter(f -> FILE_TYPE_DATA.equalsIgnoreCase(f.getFileType()))
                .count();
        long refCount = batchFiles.stream()
                .filter(f -> FILE_TYPE_REF.equalsIgnoreCase(f.getFileType()))
                .count();

        if (dataCount == 1 && refCount == 0) {
            logger.info("✅ [batchId: {}] Valid with 1 DATA file", batchId);
        } else if (dataCount > 1) {
            logger.error("❌ [batchId: {}] Rejected - Multiple DATA files", batchId);
            ack.acknowledge();
            return;
        } else if (dataCount == 0 && refCount > 0) {
            logger.error("❌ [batchId: {}] Rejected - Only REF files", batchId);
            ack.acknowledge();
            return;
        } else if (dataCount == 1 && refCount > 0) {
            logger.info("✅ [batchId: {}] Valid with DATA + REF files (both will be passed to OT)", batchId);
            message.setBatchFiles(batchFiles);
        } else {
            logger.error("❌ [batchId: {}] Rejected - Invalid or unsupported file type combination", batchId);
            ack.acknowledge();
            return;
        }

        String sanitizedBatchId = batchId.replaceAll(FILENAME_SANITIZE_REGEX, REPLACEMENT_UNDERSCORE);
        String sanitizedSourceSystem = message.getSourceSystem().replaceAll(FILENAME_SANITIZE_REGEX, REPLACEMENT_UNDERSCORE);

        Path batchDir = Paths.get(mountPath, INPUT_FOLDER, sanitizedSourceSystem, sanitizedBatchId);
        if (Files.exists(batchDir)) {
            logger.warn("⚠️ [batchId: {}] Directory already exists at path: {}", batchId, batchDir);
        } else {
            Files.createDirectories(batchDir);
            logger.info("📁 [batchId: {}] Created input directory: {}", batchId, batchDir);
        }

        for (BatchFile file : message.getBatchFiles()) {
            String blobUrl = file.getBlobUrl();
            Path localPath = batchDir.resolve(file.getFilename());
            blobStorageService.downloadFileToLocal(blobUrl, localPath);
            file.setBlobUrl(localPath.toString());
            logger.info("⬇️ [batchId: {}] Downloaded file: {} to {}", batchId, blobUrl, localPath);
        }

        String sourceSystem = message.getSourceSystem();

        if (SOURCE_DEBTMAN.equalsIgnoreCase(sourceSystem) && (otOrchestrationApiUrl == null || otOrchestrationApiUrl.isBlank())) {
            logger.error("❌ [batchId: {}] otOrchestrationApiUrl is not configured for 'DEBTMAN'", batchId);
            throw new IllegalArgumentException(ERROR_DEBTMAN_URL_NOT_CONFIGURED);
        }

        if (SOURCE_MFC.equalsIgnoreCase(sourceSystem) && (orchestrationMfcUrl == null || orchestrationMfcUrl.isBlank())) {
            logger.error("❌ [batchId: {}] orchestrationMfcUrl is not configured for 'MFC'", batchId);
            throw new IllegalArgumentException(ERROR_MFC_URL_NOT_CONFIGURED);
        }

        String url = switch (sourceSystem.toUpperCase()) {
            case SOURCE_DEBTMAN -> otOrchestrationApiUrl;
            case SOURCE_MFC -> orchestrationMfcUrl;
            default -> {
                logger.error("❌ [batchId: {}] Unsupported source system '{}'", batchId, sourceSystem);
                throw new IllegalArgumentException("Unsupported source system: " + sourceSystem);
            }
        };

        if (url == null || url.isBlank()) {
            logger.error("❌ [batchId: {}] Orchestration URL not configured for source system '{}'", batchId, sourceSystem);
            ack.acknowledge();
            return;
        }

        // ✅ Acknowledge before async OT call
        ack.acknowledge();

        String finalBatchId = batchId;
        executor.submit(() -> {
            try {
                logger.info("🚀 [batchId: {}] Calling Orchestration API: {}", finalBatchId, url);
                OTResponse otResponse = callOrchestrationBatchApi(orchestrationAuthToken, url, message);
                logger.info("📤 [batchId: {}] OT request sent successfully", finalBatchId);
                processAfterOT(message, otResponse);
            } catch (Exception ex) {
                logger.error("❌ [batchId: {}] Error during async OT or post-processing: {}", finalBatchId, ex.getMessage(), ex);
            }
        });

    } catch (Exception ex) {
        logger.error("❌ [batchId: {}] Kafka message processing failed. Error: {}", batchId, ex.getMessage(), ex);
        ack.acknowledge();
    }
}


package com.nedbank.kafka.filemanage.common;

public class AppConstants {

    private AppConstants() {} // prevent instantiation

    public static final String INPUT_FOLDER = "input";

    public static final String SOURCE_DEBTMAN = "DEBTMAN";
    public static final String SOURCE_MFC = "MFC";

    public static final String FILENAME_SANITIZE_REGEX = "[^a-zA-Z0-9_-]";
    public static final String REPLACEMENT_UNDERSCORE = "_";

    public static final String FILE_TYPE_DATA = "DATA";
    public static final String FILE_TYPE_REF = "REF";

    public static final String ERROR_DEBTMAN_URL_NOT_CONFIGURED = "otOrchestrationApiUrl is not configured";
    public static final String ERROR_MFC_URL_NOT_CONFIGURED = "orchestrationMfcUrl is not configured";

    public static final String OUTPUT_FOLDER = "output";
    public static final String SUMMARY_FILENAME_PREFIX = "summary_";
    public static final String JSON_EXTENSION = ".json";

    public static final String TRIGGER_FILE_EXTENSION = ".trigger";
    public static final String MOBSTAT_TRIGGER_UPLOAD_PATH_FORMAT = "%s/%s/%s";
}

private List<CustomerSummary> parseSTDXml(File xmlFile, Map<String, Map<String, String>> errorMap) {
    List<CustomerSummary> customerSummaries = new ArrayList<>();

    try {
        DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
        Document document = builder.parse(xmlFile);
        document.getDocumentElement().normalize();

        NodeList customerNodes = document.getElementsByTagName("customer");

        for (int i = 0; i < customerNodes.getLength(); i++) {
            Element customerElement = (Element) customerNodes.item(i);

            String accountNumber = null;
            String cisNumber = null;
            List<String> deliveryMethods = new ArrayList<>();

            NodeList keyNodes = customerElement.getElementsByTagName("key");
            for (int j = 0; j < keyNodes.getLength(); j++) {
                Element keyElement = (Element) keyNodes.item(j);
                String keyName = keyElement.getAttribute("name");

                if ("AccountNumber".equalsIgnoreCase(keyName)) {
                    accountNumber = keyElement.getTextContent();
                } else if ("CISNumber".equalsIgnoreCase(keyName)) {
                    cisNumber = keyElement.getTextContent();
                }
            }

            NodeList queueNodes = customerElement.getElementsByTagName("queueName");
            for (int q = 0; q < queueNodes.getLength(); q++) {
                String method = queueNodes.item(q).getTextContent().trim().toUpperCase();
                if (!method.isEmpty()) {
                    deliveryMethods.add(method);
                }
            }

            if (accountNumber != null && cisNumber != null) {
                CustomerSummary summary = new CustomerSummary();
                summary.setAccountNumber(accountNumber);
                summary.setCisNumber(cisNumber);
                summary.setCustomerId(accountNumber);

                Map<String, String> deliveryStatusMap = errorMap.getOrDefault(accountNumber, new HashMap<>());
                summary.setDeliveryStatus(deliveryStatusMap);

                long failedCount = deliveryMethods.stream()
                        .filter(method -> "FAILED".equalsIgnoreCase(deliveryStatusMap.getOrDefault(method, "")))
                        .count();

                if (failedCount == deliveryMethods.size()) {
                    summary.setStatus("FAILED");
                } else if (failedCount > 0) {
                    summary.setStatus("PARTIAL");
                } else {
                    summary.setStatus("SUCCESS");
                }

                customerSummaries.add(summary);

                logger.debug("📋 Customer: {}, CIS: {}, Methods: {}, Failed: {}, FinalStatus: {}",
                        accountNumber, cisNumber, deliveryMethods, failedCount, summary.getStatus());
            }
        }

    } catch (Exception e) {
        logger.error("❌ Failed parsing STD XML file: {}", xmlFile.getAbsolutePath(), e);
        throw new RuntimeException("Failed to parse XML file: " + xmlFile.getName(), e);
    }

    return customerSummaries;
}

private void processAfterOT(KafkaMessage message, OTResponse otResponse) {
    String batchId = message.getBatchId(); // golden thread
    try {
        logger.info("[{}] ⏳ Waiting for XML for jobId={}, id={}", batchId, otResponse.getJobId(), otResponse.getId());
        File xmlFile = waitForXmlFile(otResponse.getJobId(), otResponse.getId());
        if (xmlFile == null) throw new IllegalStateException("XML not found");

        logger.info("[{}] ✅ Found XML file: {}", batchId, xmlFile);

        Map<String, Map<String, String>> errorMap = parseErrorReport(message);
        logger.info("[{}] 🧾 Parsed error report with {} entries", batchId, errorMap.size());

        List<CustomerSummary> customerSummaries = parseSTDXml(xmlFile, errorMap);
        logger.info("[{}] 📊 Total customerSummaries parsed: {}", batchId, customerSummaries.size());

        List<SummaryProcessedFile> customerList = customerSummaries.stream()
                .map(cs -> {
                    SummaryProcessedFile spf = new SummaryProcessedFile();
                    spf.setAccountNumber(cs.getAccountNumber());
                    spf.setCustomerId(cs.getCisNumber());
                    return spf;
                })
                .collect(Collectors.toList());

        Path jobDir = Paths.get(mountPath, AppConstants.OUTPUT_FOLDER, message.getSourceSystem(), otResponse.getJobId());

        List<SummaryProcessedFile> processedFiles =
                buildDetailedProcessedFiles(jobDir, customerList, errorMap, message);
        logger.info("[{}] 📦 Processed {} customer records", batchId, processedFiles.size());

        List<PrintFile> printFiles = uploadPrintFiles(jobDir, message);
        logger.info("[{}] 🖨️ Uploaded {} print files", batchId, printFiles.size());

        String mobstatTriggerUrl = findAndUploadMobstatTriggerFile(jobDir, message);
        logger.info("[{}] 📱 Found Mobstat URL: {}", batchId, mobstatTriggerUrl);

        Map<String, Integer> summaryCounts = extractSummaryCountsFromXml(xmlFile);

        String allFileNames = message.getBatchFiles().stream()
                .map(BatchFile::getFilename)
                .collect(Collectors.joining(", "));

        SummaryPayload payload = SummaryJsonWriter.buildPayload(
                message, processedFiles, allFileNames, batchId,
                String.valueOf(message.getTimestamp()), errorMap, printFiles
        );

        if (payload.getHeader() != null) {
            payload.getHeader().setTimestamp(String.valueOf(message.getTimestamp()));
        }

        String fileName = AppConstants.SUMMARY_FILENAME_PREFIX + batchId + AppConstants.JSON_EXTENSION;
        String summaryPath = SummaryJsonWriter.writeSummaryJsonToFile(payload);
        String summaryUrl = blobStorageService.uploadSummaryJson(summaryPath, message, fileName);
        payload.setSummaryFileURL(decodeUrl(summaryUrl));
        logger.info("[{}] 📁 Summary JSON uploaded to: {}", batchId, decodeUrl(summaryUrl));

        logger.info("[{}] 📄 Final Summary Payload:\n{}", batchId,
                objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(payload));

        SummaryResponse response = new SummaryResponse();
        response.setBatchID(batchId);
        response.setFileName(payload.getFileName());
        response.setHeader(payload.getHeader());
        response.setMetadata(payload.getMetadata());
        response.setPayload(payload.getPayload());
        response.setSummaryFileURL(decodeUrl(summaryUrl));
        response.setTimestamp(String.valueOf(message.getTimestamp()));

        kafkaTemplate.send(kafkaOutputTopic, objectMapper.writeValueAsString(
                new ApiResponse("Summary generated", "COMPLETED", response)));

        logger.info("[{}] ✅ Kafka output sent with response: {}", batchId,
                objectMapper.writeValueAsString(response));

    } catch (Exception e) {
        logger.error("[{}] ❌ Error post-OT summary generation: {}", batchId, e.getMessage(), e);
    }
}

private String findAndUploadMobstatTriggerFile(Path jobDir, KafkaMessage message) {
    try (Stream<Path> stream = Files.list(jobDir)) {
        Optional<Path> trigger = stream.filter(Files::isRegularFile)
                .filter(p -> p.getFileName().toString().toLowerCase().endsWith(TRIGGER_FILE_EXTENSION))
                .findFirst();

        if (trigger.isPresent()) {
            Path triggerFile = trigger.get();
            String blobUrl = blobStorageService.uploadFile(
                triggerFile.toFile(),
                String.format(MOBSTAT_TRIGGER_UPLOAD_PATH_FORMAT,
                    message.getSourceSystem(), message.getBatchId(), triggerFile.getFileName())
            );

            logger.info("📤 Uploaded MOBSTAT trigger file: {} -> {}", triggerFile, blobUrl);
            return decodeUrl(blobUrl);
        } else {
            logger.warn("⚠️ No .trigger file found in MOBSTAT job directory: {}", jobDir);
            return null;
        }

    } catch (IOException e) {
        logger.error("⚠️ Failed to scan for .trigger file in jobDir: {}", jobDir, e);
        return null;
    }
}


private OTResponse callOrchestrationBatchApi(String token, String url, KafkaMessage msg) {
    OTResponse otResponse = new OTResponse();
    try {
        logger.info("📡 Initiating OT orchestration call to URL: {} for batchId: {} and sourceSystem: {}",
                url, msg.getBatchId(), msg.getSourceSystem());

        HttpHeaders headers = new HttpHeaders();
        headers.set(AppConstants.HEADER_AUTHORIZATION, AppConstants.BEARER_PREFIX + token);
        headers.setContentType(MediaType.APPLICATION_JSON);

        HttpEntity<String> request = new HttpEntity<>(objectMapper.writeValueAsString(msg), headers);
        logger.debug("📨 OT Request Payload: {}", objectMapper.writeValueAsString(msg));

        ResponseEntity<Map> response = restTemplate.exchange(url, HttpMethod.POST, request, Map.class);
        logger.info("✅ Received OT response with status: {} for batchId: {}",
                response.getStatusCode(), msg.getBatchId());

        List<Map<String, Object>> data = (List<Map<String, Object>>) response.getBody().get(AppConstants.OT_RESPONSE_DATA_KEY);
        if (data != null && !data.isEmpty()) {
            Map<String, Object> item = data.get(0);
            otResponse.setJobId((String) item.get(AppConstants.OT_JOB_ID_KEY));
            otResponse.setId((String) item.get(AppConstants.OT_ID_KEY));
            msg.setJobName(otResponse.getJobId());
            otResponse.setSuccess(true);

            logger.info("🎯 OT Job created successfully - JobID: {}, ID: {}, BatchID: {}",
                    otResponse.getJobId(), otResponse.getId(), msg.getBatchId());
        } else {
            logger.error("❌ No data found in OT orchestration response for batchId: {}", msg.getBatchId());
            otResponse.setSuccess(false);
            otResponse.setMessage(AppConstants.NO_OT_DATA_MESSAGE);
        }

        return otResponse;
    } catch (Exception e) {
        logger.error("❌ Exception during OT orchestration call for batchId: {} - {}",
                msg.getBatchId(), e.getMessage(), e);
        otResponse.setSuccess(false);
        otResponse.setMessage(AppConstants.OT_CALL_FAILURE_PREFIX + e.getMessage());
        return otResponse;
    }
}

public class AppConstants {
    public static final String HEADER_AUTHORIZATION = "Authorization";
    public static final String BEARER_PREFIX = "Bearer ";
    public static final String APPLICATION_JSON = "application/json";
    public static final String OT_RESPONSE_DATA_KEY = "data";
    public static final String OT_JOB_ID_KEY = "jobId";
    public static final String OT_ID_KEY = "id";
    public static final String NO_OT_DATA_MESSAGE = "No data in OT orchestration response";
    public static final String OT_CALL_FAILURE_PREFIX = "Failed OT orchestration call: ";

    public static final String DOCGEN_FOLDER = "docgen";
    public static final String XML_FILE_NAME = "_STDDELIVERYFILE.xml";
    public static final String LOG_FOUND_STABLE_XML = "✅ Found stable XML file: {}";
    public static final String LOG_XML_SIZE_CHANGING = "⌛ XML file still being written (size changing): {}";
    public static final String LOG_DOCGEN_FOLDER_NOT_FOUND = "🔍 docgen folder not found yet for jobId={}, id={}: {}";
    public static final String LOG_ERROR_SCANNING_FOLDER = "⚠️ Error scanning docgen folder for jobId={} id={}: {}";
    public static final String LOG_XML_TIMEOUT = "❌ Timeout while waiting for stable XML file (_STDDELIVERYFILE.xml) in path: %s for jobId=%s, id=%s";
}

private File waitForXmlFile(String jobId, String id) throws InterruptedException {
    Path docgenRoot = Paths.get(mountPath, "jobs", jobId, id, AppConstants.DOCGEN_FOLDER);
    long startTime = System.currentTimeMillis();
    File xmlFile = null;

    while ((System.currentTimeMillis() - startTime) < rptMaxWaitSeconds * 1000L) {
        if (Files.exists(docgenRoot)) {
            try (Stream<Path> paths = Files.walk(docgenRoot)) {
                Optional<Path> xmlPath = paths
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().equalsIgnoreCase(AppConstants.XML_FILE_NAME))
                        .findFirst();

                if (xmlPath.isPresent()) {
                    xmlFile = xmlPath.get().toFile();

                    long size1 = xmlFile.length();
                    TimeUnit.SECONDS.sleep(1);
                    long size2 = xmlFile.length();

                    if (size1 > 0 && size1 == size2) {
                        logger.info(AppConstants.LOG_FOUND_STABLE_XML, xmlFile.getAbsolutePath());
                        return xmlFile;
                    } else {
                        logger.info(AppConstants.LOG_XML_SIZE_CHANGING, xmlFile.getAbsolutePath());
                    }
                }
            } catch (IOException e) {
                logger.warn(AppConstants.LOG_ERROR_SCANNING_FOLDER, jobId, id, e.getMessage(), e);
            }
        } else {
            logger.debug(AppConstants.LOG_DOCGEN_FOLDER_NOT_FOUND, jobId, id, docgenRoot);
        }

        TimeUnit.MILLISECONDS.sleep(rptPollIntervalMillis);
    }

    String errMsg = String.format(AppConstants.LOG_XML_TIMEOUT, docgenRoot, jobId, id);
    logger.error(errMsg);
    throw new IllegalStateException(errMsg);
}

private Map<String, Map<String, String>> parseErrorReport(KafkaMessage msg) {
    Map<String, Map<String, String>> result = new HashMap<>();
    try {
        String jobRootPath = mountPath + "/jobs";
        Path jobRoot = Paths.get(jobRootPath);
        String jobId = msg.getJobName();

        // Find all ErrorReport.csv files under this job's path
        Path jobPath = jobRoot.resolve(jobId);
        if (!Files.exists(jobPath)) {
            logger.warn("❌ Job path not found: {}", jobPath);
            return result;
        }

        logger.info("🔍 Searching ErrorReport.csv under: {}", jobPath);

        try (Stream<Path> stream = Files.walk(jobPath)) {
            Optional<Path> reportFile = stream
                    .filter(path -> path.getFileName().toString().equalsIgnoreCase(AppConstants.ERROR_REPORT_FILE_NAME))
                    .findFirst();

            if (reportFile.isEmpty()) {
                logger.warn("⚠️ ErrorReport.csv not found under job {}", jobId);
                return result;
            }

            Path reportPath = reportFile.get();
            logger.info("✅ Found ErrorReport.csv at: {}", reportPath);

            try (BufferedReader reader = Files.newBufferedReader(reportPath)) {
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] parts = line.split("\\|");

                    if (parts.length >= 3) {
                        String account = parts[AppConstants.ERROR_REPORT_INDEX_ACCOUNT].trim();

                        String method = AppConstants.DEFAULT_METHOD;
                        if (parts.length >= 4) {
                            method = parts[AppConstants.ERROR_REPORT_INDEX_METHOD_V1].trim();
                        } else if (parts.length >= 3) {
                            method = parts[AppConstants.ERROR_REPORT_INDEX_METHOD_V2].trim();
                        }

                        String status = AppConstants.DEFAULT_STATUS;
                        if (parts.length >= 5) {
                            status = parts[AppConstants.ERROR_REPORT_INDEX_STATUS_V1].trim();
                        } else if (parts.length >= 4) {
                            status = parts[AppConstants.ERROR_REPORT_INDEX_STATUS_V2].trim();
                        }

                        if (method.isEmpty()) method = AppConstants.DEFAULT_METHOD;
                        if (status.isEmpty()) status = AppConstants.DEFAULT_STATUS;

                        result.computeIfAbsent(account, k -> new HashMap<>()).put(method.toUpperCase(), status);
                    }
                }
            } catch (IOException e) {
                logger.error("❌ Error reading ErrorReport.csv: {}", e.getMessage(), e);
            }
        }
    } catch (Exception ex) {
        logger.error("❌ Failed to parse error report: {}", ex.getMessage(), ex);
    }

    return result;
}

public class AppConstants {
    public static final String ERROR_REPORT_FILE_NAME = "ErrorReport.csv";
    public static final String DEFAULT_METHOD = "UNKNOWN";
    public static final String DEFAULT_STATUS = "Failed";

    // Indexes in ErrorReport.csv
    public static final int ERROR_REPORT_INDEX_ACCOUNT = 0;
    public static final int ERROR_REPORT_INDEX_METHOD_V1 = 3; // If length >= 4
    public static final int ERROR_REPORT_INDEX_METHOD_V2 = 2; // fallback
    public static final int ERROR_REPORT_INDEX_STATUS_V1 = 4; // If length >= 5
    public static final int ERROR_REPORT_INDEX_STATUS_V2 = 3; // fallback
}

private List<SummaryProcessedFile> buildDetailedProcessedFiles(
        Path jobDir,
        List<SummaryProcessedFile> customerList,
        Map<String, Map<String, String>> errorMap,
        KafkaMessage msg) throws IOException {

    List<SummaryProcessedFile> finalList = new ArrayList<>();
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

    if (jobDir == null || customerList == null || msg == null) {
        logger.warn("⚠️ One or more input parameters are null: jobDir={}, customerList={}, msg={}",
                jobDir, customerList, msg);
        return finalList;
    }

    Path archivePath = jobDir.resolve(AppConstants.FOLDER_ARCHIVE);

    for (SummaryProcessedFile customer : customerList) {
        if (customer == null) continue;

        String account = customer.getAccountNumber();
        if (account == null || account.isBlank()) {
            logger.warn("⚠️ Skipping customer with empty account number");
            continue;
        }

        // Archive upload
        String archiveBlobUrl = null;
        try {
            if (Files.exists(archivePath)) {
                Optional<Path> archiveFile = Files.list(archivePath)
                        .filter(Files::isRegularFile)
                        .filter(p -> p.getFileName().toString().contains(account))
                        .findFirst();

                if (archiveFile.isPresent()) {
                    archiveBlobUrl = blobStorageService.uploadFileByMessage(
                            archiveFile.get().toFile(), AppConstants.FOLDER_ARCHIVE, msg);

                    SummaryProcessedFile archiveEntry = new SummaryProcessedFile();
                    BeanUtils.copyProperties(customer, archiveEntry);
                    archiveEntry.setOutputType(AppConstants.OUTPUT_ARCHIVE);
                    archiveEntry.setBlobUrl(decodeUrl(archiveBlobUrl));

                    finalList.add(archiveEntry);
                    logger.info("📦 Uploaded archive file for account {}: {}", account, archiveBlobUrl);
                }
            }
        } catch (Exception e) {
            logger.warn("⚠️ Failed to upload archive file for account {}: {}", account, e.getMessage(), e);
        }

        // EMAIL, MOBSTAT, PRINT uploads
        for (String folder : deliveryFolders) {
            String outputMethod = folderToOutputMethod.get(folder);
            Path methodPath = jobDir.resolve(folder);
            String blobUrl = null;

            try {
                if (Files.exists(methodPath)) {
                    Optional<Path> match = Files.list(methodPath)
                            .filter(Files::isRegularFile)
                            .filter(p -> p.getFileName().toString().contains(account))
                            .findFirst();

                    if (match.isPresent()) {
                        blobUrl = blobStorageService.uploadFileByMessage(match.get().toFile(), folder, msg);
                        logger.info("✅ Uploaded {} file for account {}: {}", outputMethod, account, blobUrl);
                    }
                }
            } catch (Exception e) {
                logger.warn("⚠️ Failed to upload {} file for account {}: {}", outputMethod, account, e.getMessage(), e);
            }

            SummaryProcessedFile entry = new SummaryProcessedFile();
            BeanUtils.copyProperties(customer, entry);
            entry.setOutputType(outputMethod);
            entry.setBlobUrl(decodeUrl(blobUrl));

            if (archiveBlobUrl != null) {
                entry.setArchiveOutputType(AppConstants.OUTPUT_ARCHIVE);
                entry.setArchiveBlobUrl(archiveBlobUrl);
            }

            finalList.add(entry);
        }
    }

    return finalList;
}

public class AppConstants {
    public static final String FOLDER_EMAIL = "email";
    public static final String FOLDER_MOBSTAT = "mobstat";
    public static final String FOLDER_PRINT = "print";
    public static final String FOLDER_ARCHIVE = "archive";

    public static final String OUTPUT_EMAIL = "EMAIL";
    public static final String OUTPUT_MOBSTAT = "MOBSTAT";
    public static final String OUTPUT_PRINT = "PRINT";
    public static final String OUTPUT_ARCHIVE = "ARCHIVE";

// Newly added constants
    public static final String PRINT_FOLDER_NAME = "print";
    public static final String UNKNOWN_FILE_NAME = "unknown_file";
}


private List<PrintFile> uploadPrintFiles(Path jobDir, KafkaMessage msg) {
    List<PrintFile> printFiles = new ArrayList<>();

    if (jobDir == null || msg == null || msg.getSourceSystem() == null) {
        logger.error("❌ Invalid input: jobDir={}, msg={}, sourceSystem={}", jobDir, msg, msg != null ? msg.getSourceSystem() : null);
        return printFiles;
    }

    Path printDir = jobDir.resolve(AppConstants.PRINT_FOLDER_NAME);
    if (!Files.exists(printDir)) {
        logger.info("ℹ️ No '{}' directory found in jobDir: {}", AppConstants.PRINT_FOLDER_NAME, jobDir);
        return printFiles;
    }

    try (Stream<Path> stream = Files.list(printDir)) {
        stream.filter(Files::isRegularFile).forEach(f -> {
            try {
                String fileName = f.getFileName() != null ? f.getFileName().toString() : AppConstants.UNKNOWN_FILE_NAME;
                String uploadPath = msg.getSourceSystem() + "/" + AppConstants.PRINT_FOLDER_NAME + "/" + fileName;

                String blob = blobStorageService.uploadFile(f.toFile(), uploadPath);
                printFiles.add(new PrintFile(blob));

                logger.info("📤 Uploaded print file: {} -> {}", fileName, blob);
            } catch (Exception e) {
                logger.warn("⚠️ Failed to upload print file: {}", f, e);
            }
        });
    } catch (IOException e) {
        logger.error("❌ Failed to list files in '{}' directory: {}", AppConstants.PRINT_FOLDER_NAME, printDir, e);
    }

    return printFiles;
}

public static final String CUSTOMERS_PROCESSED_KEY = "customersProcessed";
public static final String PAGES_PROCESSED_KEY = "pagesProcessed";

private Map<String, Integer> extractSummaryCountsFromXml(File xmlFile) {
    Map<String, Integer> summaryCounts = new HashMap<>();

    if (xmlFile == null || !xmlFile.exists() || !xmlFile.canRead()) {
        logger.warn("⚠️ Invalid or unreadable XML file: {}", xmlFile);
        return summaryCounts;
    }

    try {
        Document doc = DocumentBuilderFactory.newInstance()
                .newDocumentBuilder()
                .parse(xmlFile);
        doc.getDocumentElement().normalize();

        NodeList outputListNodes = doc.getElementsByTagName("outputList");
        if (outputListNodes.getLength() > 0) {
            Element outputList = (Element) outputListNodes.item(0);
            String customersProcessed = outputList.getAttribute(AppConstants.CUSTOMERS_PROCESSED_KEY);
            String pagesProcessed = outputList.getAttribute(AppConstants.PAGES_PROCESSED_KEY);

            int custCount = (customersProcessed != null && !customersProcessed.isBlank())
                    ? Integer.parseInt(customersProcessed.trim()) : 0;
            int pageCount = (pagesProcessed != null && !pagesProcessed.isBlank())
                    ? Integer.parseInt(pagesProcessed.trim()) : 0;

            summaryCounts.put(AppConstants.CUSTOMERS_PROCESSED_KEY, custCount);
            summaryCounts.put(AppConstants.PAGES_PROCESSED_KEY, pageCount);

            logger.info("📄 Extracted summary counts from {}: customersProcessed={}, pagesProcessed={}",
                    xmlFile.getName(), custCount, pageCount);
        } else {
            logger.info("ℹ️ No <outputList> found in XML: {}", xmlFile.getName());
        }
    } catch (Exception e) {
        logger.warn("⚠️ Unable to extract summary counts from XML file: {}", xmlFile.getName(), e);
    }

    return summaryCounts;
}
