/**
 * Main Kafka Listener Service
 */
public ApiResponse listen() {
    Properties props = new Properties();
    props.put("bootstrap.servers", bootstrapServers);
    props.put("group.id", consumerGroupId);
    props.put("enable.auto.commit", enableAutoCommit);
    props.put("auto.offset.reset", autoOffsetReset);
    props.put("key.deserializer", keyDeserializer);
    props.put("value.deserializer", valueDeserializer);
    props.put("security.protocol", securityProtocol);
    props.put("ssl.truststore.location", truststoreLocation);
    props.put("ssl.truststore.password", truststorePassword);
    props.put("ssl.keystore.location", keystoreLocation);
    props.put("ssl.keystore.password", keystorePassword);
    props.put("ssl.key.password", keyPassword);
    props.put("ssl.protocol", sslProtocol);
    props.put("ssl.endpoint.identification.algorithm", "");

    try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
        TopicPartition partition = new TopicPartition(inputTopic, 0);
        consumer.assign(Collections.singletonList(partition));

        OffsetAndMetadata committed = consumer.committed(partition);
        long nextOffset = committed != null ? committed.offset() : 0;

        consumer.seek(partition, nextOffset);

        ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
        if (records.isEmpty()) {
            logger.info("No new messages at offset {}", nextOffset);
            return new ApiResponse(
                    "No new messages to process",
                    "info",
                    new SummaryPayloadResponse("No new messages to process", "info", new SummaryResponse()).getSummaryResponse()
            );
        }

        for (ConsumerRecord<String, String> record : records) {
            try {
                logger.info("Processing Kafka message at offset {}", record.offset());
                KafkaMessage kafkaMessage = objectMapper.readValue(record.value(), KafkaMessage.class);
                ApiResponse response = processSingleMessage(kafkaMessage);
                kafkaTemplate.send(outputTopic, objectMapper.writeValueAsString(response));
                consumer.commitSync(Collections.singletonMap(
                        partition,
                        new OffsetAndMetadata(record.offset() + 1)
                ));
                logger.info("Committed Kafka offset {}", record.offset() + 1);
                return response;
            } catch (Exception ex) {
                logger.error("Error processing Kafka message", ex);
                return new ApiResponse(
                        "Error processing message: " + ex.getMessage(),
                        "error",
                        new SummaryPayloadResponse("Error processing message", "error", new SummaryResponse()).getSummaryResponse()
                );
            }
        }
    } catch (Exception e) {
        logger.error("Kafka consumer failed", e);
        return new ApiResponse(
                "Kafka error: " + e.getMessage(),
                "error",
                new SummaryPayloadResponse("Kafka error", "error", new SummaryResponse()).getSummaryResponse()
        );
    }

    return new ApiResponse(
            "No messages processed",
            "info",
            new SummaryPayloadResponse("No messages processed", "info", new SummaryResponse()).getSummaryResponse()
    );
}

/**
 * Processes a single Kafka message and generates output files + summary.json
 */
private ApiResponse processSingleMessage(KafkaMessage message) throws UnsupportedEncodingException {
    if (message == null) {
        return new ApiResponse("Empty message", "error",
                new SummaryPayloadResponse("Empty message", "error", new SummaryResponse()).getSummaryResponse());
    }

    Header header = new Header();
    header.setTenantCode(message.getTenantCode());
    header.setChannelID(message.getChannelID());
    header.setAudienceID(message.getAudienceID());
    header.setTimestamp(instantToIsoString(message.getTimestamp()));
    header.setSourceSystem(message.getSourceSystem());
    header.setProduct(message.getProduct());
    header.setJobName(message.getJobName());

    Payload payload = new Payload();
    payload.setUniqueConsumerRef(message.getUniqueConsumerRef());
    payload.setRunPriority(message.getRunPriority());
    payload.setEventType(message.getEventType());

    List<SummaryProcessedFile> processedFiles = new ArrayList<>();
    List<PrintFile> printFiles = new ArrayList<>();
    Metadata metadata = new Metadata();
    String summaryFileUrl = "";

    String fileName = null;
    if (message.getBatchFiles() != null && !message.getBatchFiles().isEmpty()) {
        String firstBlobUrl = message.getBatchFiles().get(0).getBlobUrl();
        String blobPath = extractBlobPath(firstBlobUrl);
        fileName = extractFileName(blobPath);
    }
    if (fileName == null || fileName.isEmpty()) {
        fileName = message.getBatchId() + "_summary.json";
    }

    for (BatchFile file : message.getBatchFiles()) {
        try {
            String inputFileContent = blobStorageService.downloadFileContent(extractFileName(extractBlobPath(file.getBlobUrl())));
            List<CustomerData> customers = DataParser.extractCustomerData(inputFileContent);
            if (customers.isEmpty()) continue;

            for (CustomerData customer : customers) {
                File pdfFile = FileGenerator.generatePdf(customer);
                File htmlFile = FileGenerator.generateHtml(customer);
                File txtFile = FileGenerator.generateTxt(customer);
                File mobstatFile = FileGenerator.generateMobstat(customer);

                String pdfArchiveUrl = blobStorageService.uploadFile(pdfFile.getAbsolutePath(),
                        buildBlobPath(message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                                message.getUniqueConsumerRef(), message.getJobName(), "archive",
                                customer.getAccountNumber(), pdfFile.getName())).split("\\?")[0];

                String pdfEmailUrl = blobStorageService.uploadFile(pdfFile.getAbsolutePath(),
                        buildBlobPath(message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                                message.getUniqueConsumerRef(), message.getJobName(), "email",
                                customer.getAccountNumber(), pdfFile.getName())).split("\\?")[0];

                String htmlEmailUrl = blobStorageService.uploadFile(htmlFile.getAbsolutePath(),
                        buildBlobPath(message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                                message.getUniqueConsumerRef(), message.getJobName(), "html",
                                customer.getAccountNumber(), htmlFile.getName())).split("\\?")[0];

                String txtEmailUrl = blobStorageService.uploadFile(txtFile.getAbsolutePath(),
                        buildBlobPath(message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                                message.getUniqueConsumerRef(), message.getJobName(), "txt",
                                customer.getAccountNumber(), txtFile.getName())).split("\\?")[0];

                String mobstatUrl = blobStorageService.uploadFile(mobstatFile.getAbsolutePath(),
                        buildBlobPath(message.getSourceSystem(), message.getTimestamp(), message.getBatchId(),
                                message.getUniqueConsumerRef(), message.getJobName(), "mobstat",
                                customer.getAccountNumber(), mobstatFile.getName())).split("\\?")[0];

                SummaryProcessedFile processedFile = new SummaryProcessedFile();
                processedFile.setCustomerId(customer.getCustomerId());
                processedFile.setAccountNumber(customer.getAccountNumber());
                processedFile.setPdfArchiveFileUrl(pdfArchiveUrl);
                processedFile.setPdfEmailFileUrl(pdfEmailUrl);
                processedFile.setHtmlEmailFileUrl(htmlEmailUrl);
                processedFile.setTxtEmailFileUrl(txtEmailUrl);
                processedFile.setPdfMobstatFileUrl(mobstatUrl);
                processedFile.setStatusCode("OK");
                processedFile.setStatusDescription("Success");
                processedFiles.add(processedFile);

                logger.info("Processed customerId {}, accountNumber {}", customer.getCustomerId(), customer.getAccountNumber());
            }
        } catch (Exception ex) {
            logger.error("Error processing file '{}': {}", file.getFilename(), ex.getMessage(), ex);
        }
    }

    PrintFile printFile = new PrintFile();
    printFile.setPrintFileURL(blobStorageService.buildPrintFileUrl(message));
    printFiles.add(printFile);

    payload.setFileCount(processedFiles.size());

    metadata.setProcessingStatus("Completed");
    metadata.setTotalFilesProcessed(processedFiles.size());
    metadata.setEventOutcomeCode("0");
    metadata.setEventOutcomeDescription("Success");

    SummaryPayload summaryPayload = new SummaryPayload();
    summaryPayload.setBatchID(message.getBatchId());
    summaryPayload.setFileName(fileName);
    summaryPayload.setHeader(header);
    summaryPayload.setMetadata(metadata);
    summaryPayload.setPayload(payload);
    summaryPayload.setProcessedFiles(processedFiles);
    summaryPayload.setMobstatTriggerFile("/main/nedcor/dia/ecm-batch/testfolder/azurebloblocation/output/mobstat/DropData.trigger");
    summaryPayload.setPrintFiles(printFiles);

    // Write summary JSON
    String summaryJsonPath = SummaryJsonWriter.writeSummaryJsonToFile(summaryPayload);

    // Upload summary JSON
    String summaryFileName = "summary_" + message.getBatchId() + ".json";
    summaryFileUrl = blobStorageService.uploadSummaryJson(summaryJsonPath, message, summaryFileName);
    String decodedUrl = URLDecoder.decode(summaryFileUrl, StandardCharsets.UTF_8);
    summaryPayload.setSummaryFileURL(decodedUrl);

    SummaryResponse summaryResponse = new SummaryResponse();
    summaryResponse.setBatchID(summaryPayload.getBatchID());
    summaryResponse.setFileName(summaryPayload.getFileName());
    summaryResponse.setHeader(summaryPayload.getHeader());
    summaryResponse.setMetadata(summaryPayload.getMetadata());
    summaryResponse.setPayload(summaryPayload.getPayload());
    summaryResponse.setSummaryFileURL(summaryPayload.getSummaryFileURL());
    summaryResponse.setTimestamp(String.valueOf(Instant.now()));

    SummaryPayloadResponse apiPayload = new SummaryPayloadResponse("Batch processed successfully", "success", summaryResponse);
    return new ApiResponse(apiPayload.getMessage(), apiPayload.getStatus(), apiPayload.getSummaryResponse());
}

/**
 * Builds blob storage path
 */
private String buildBlobPath(String sourceSystem, long timestamp, String batchId,
                             String uniqueConsumerRef, String jobName, String folder,
                             String customerAccount, String fileName) {
    DateTimeFormatter dtf = DateTimeFormatter.ofPattern("yyyy-MM-dd")
            .withZone(ZoneId.systemDefault());
    String dateStr = dtf.format(Instant.ofEpochMilli(timestamp));
    return String.format("%s/%s/%s/%s/%s/%s/%s",
            sourceSystem, dateStr, batchId, uniqueConsumerRef, jobName, folder, fileName);
}

/**
 * Extracts blob path from full URL
 */
private String extractBlobPath(String fullUrl) {
    if (fullUrl == null) return "";
    try {
        URI uri = URI.create(fullUrl);
        String path = uri.getPath();
        return path.startsWith("/") ? path.substring(1) : path;
    } catch (Exception e) {
        return fullUrl;
    }
}

/**
 * Extracts file name from path or URL
 */
public String extractFileName(String fullPathOrUrl) {
    if (fullPathOrUrl == null || fullPathOrUrl.isEmpty()) return fullPathOrUrl;
    String trimmed = fullPathOrUrl.replaceAll("/+", "/");
    int lastSlashIndex = trimmed.lastIndexOf('/');
    return lastSlashIndex >= 0 ? trimmed.substring(lastSlashIndex + 1) : trimmed;
}

/**
 * Converts epoch millis to ISO timestamp
 */
private String instantToIsoString(long epochMillis) {
    return Instant.ofEpochMilli(epochMillis).toString();
}
