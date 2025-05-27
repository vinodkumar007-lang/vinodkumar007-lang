@Service
public class KafkaListenerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final BlobStorageService blobStorageService;
    private final ConsumerFactory<String, String> consumerFactory;
    private final ObjectMapper objectMapper = new ObjectMapper();

    @Value("${kafka.topic.input}")
    private String inputTopic;

    @Value("${kafka.topic.output}")
    private String outputTopic;

    @Value("${azure.blob.storage.account}")
    private String azureBlobStorageAccount;

    private final Map<TopicPartition, Long> lastProcessedOffsets = new HashMap<>();

    public KafkaListenerService(KafkaTemplate<String, String> kafkaTemplate,
                                BlobStorageService blobStorageService,
                                ConsumerFactory<String, String> consumerFactory) {
        this.kafkaTemplate = kafkaTemplate;
        this.blobStorageService = blobStorageService;
        this.consumerFactory = consumerFactory;
    }

    public ApiResponse processBatchMessages() {
        logger.info("Starting batch processing of Kafka messages...");
        List<KafkaMessage> unprocessedMessages = readUnprocessedKafkaMessages();

        if (unprocessedMessages.isEmpty()) {
            logger.info("No new messages found.");
            return buildEmptyResponse();
        }

        List<SummaryProcessedFile> processedFiles = new ArrayList<>();
        List<PrintFile> printFiles = new ArrayList<>();

        KafkaMessage firstMessage = unprocessedMessages.get(0);

        // For summary header & payload, using the first message's data
        SummaryPayload summaryPayload = new SummaryPayload();
        Header header = new Header();
        Metadata metadata = new Metadata();
        Payload payload = new Payload();

        header.setTenantCode(firstMessage.getTenantCode());
        header.setChannelID(firstMessage.getChannelID());
        header.setAudienceID(firstMessage.getAudienceID());
        header.setTimestamp(instantToIsoString(firstMessage.getTimestamp()));
        header.setSourceSystem(firstMessage.getSourceSystem());
        header.setProduct(firstMessage.getProduct());
        header.setJobName(firstMessage.getJobName());

        payload.setUniqueConsumerRef(firstMessage.getUniqueConsumerRef());
        payload.setRunPriority(firstMessage.getRunPriority());
        payload.setEventType(firstMessage.getEventType());

        int totalFilesProcessed = 0;

        // Process each Kafka message and copy files from Blob URLs
        for (KafkaMessage message : unprocessedMessages) {
            for (BatchFile batchFile : message.getBatchFiles()) {
                String targetBlobPath = buildTargetBlobPath(
                        message.getSourceSystem(),
                        message.getTimestamp(),
                        message.getBatchId(),
                        message.getUniqueConsumerRef(),
                        message.getJobName(),
                        batchFile.getFilename()
                );

                logger.info("Copying file from '{}' to '{}'", batchFile.getBlobUrl(), targetBlobPath);
                String newBlobUrl = blobStorageService.copyFileFromUrlToBlob(batchFile.getBlobUrl(), targetBlobPath);

                // For demo, creating dummy processedFile data:
                SummaryProcessedFile spf = new SummaryProcessedFile();
                spf.setCustomerID("C001");
                spf.setAccountNumber("123456780123456");
                spf.setPdfArchiveFileURL(generatePdfUrl("archive", "123456780123456", message.getBatchId()));
                spf.setPdfEmailFileURL(generatePdfUrl("email", "123456780123456", message.getBatchId()));
                spf.setHtmlEmailFileURL(generatePdfUrl("html", "123456780123456", message.getBatchId()));
                spf.setTxtEmailFileURL(generatePdfUrl("txt", "123456780123456", message.getBatchId()));
                spf.setPdfMobstatFileURL(generatePdfUrl("mobstat", "123456780123456", message.getBatchId()));
                spf.setStatusCode("OK");
                spf.setStatusDescription("Success");
                processedFiles.add(spf);

                totalFilesProcessed++;
            }

            // Simulate adding some print files for demo
            PrintFile pf = new PrintFile();
            pf.setPrintFileURL("https://" + azureBlobStorageAccount + "/pdfs/mobstat/PrintFileName1_" + message.getBatchId() + ".ps");
            printFiles.add(pf);
        }

        metadata.setTotalFilesProcessed(totalFilesProcessed);
        metadata.setProcessingStatus("Completed");
        metadata.setEventOutcomeCode("200");
        metadata.setEventOutcomeDescription("Batch processed successfully");

        summaryPayload.setBatchID(firstMessage.getBatchId());
        summaryPayload.setFileName("summary_" + firstMessage.getBatchId() + ".json");
        summaryPayload.setHeader(header);
        summaryPayload.setMetadata(metadata);
        summaryPayload.setPayload(payload);
        summaryPayload.setProcessedFiles(processedFiles);
        summaryPayload.setPrintFiles(printFiles);

        // Write summary JSON locally
        File summaryFile = new File(System.getProperty("java.io.tmpdir"), "summary.json");
        try {
            objectMapper.writerWithDefaultPrettyPrinter().writeValue(summaryFile, summaryPayload);
            logger.info("Summary JSON written locally at {}", summaryFile.getAbsolutePath());
        } catch (IOException e) {
            logger.error("Failed to write summary.json", e);
            throw new RuntimeException(e);
        }

        // Upload summary.json to blob storage
        String summaryBlobPath = String.format("%s/summary/%s/summary.json",
                firstMessage.getSourceSystem(), firstMessage.getBatchId());
        String summaryFileUrl = blobStorageService.uploadFile(summaryFile, summaryBlobPath);
        summaryPayload.setSummaryFileURL(summaryFileUrl);

        // Prepare API response
        ApiResponse apiResponse = new ApiResponse();
        apiResponse.setMessage("Batch processed successfully");
        apiResponse.setStatus("success");
        apiResponse.setSummaryPayload(summaryPayload);

        // Send final response as Kafka producer message
        try {
            String apiResponseJson = objectMapper.writeValueAsString(apiResponse);
            kafkaTemplate.send(outputTopic, apiResponseJson);
            logger.info("Sent final API response to Kafka topic {}", outputTopic);
        } catch (Exception e) {
            logger.error("Failed to send response message to Kafka", e);
        }

        return apiResponse;
    }

    private List<KafkaMessage> readUnprocessedKafkaMessages() {
        logger.info("Reading unprocessed Kafka messages from topic: {}", inputTopic);
        List<KafkaMessage> messages = new ArrayList<>();

        try (Consumer<String, String> consumer = consumerFactory.createConsumer()) {
            consumer.assign(Collections.singletonList(new TopicPartition(inputTopic, 0)));

            // Seek to last processed offset + 1 or beginning
            long offset = lastProcessedOffsets.getOrDefault(new TopicPartition(inputTopic, 0), -1L);
            if (offset >= 0) {
                consumer.seek(new TopicPartition(inputTopic, 0), offset + 1);
            } else {
                consumer.seekToBeginning(Collections.singletonList(new TopicPartition(inputTopic, 0)));
            }

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            logger.info("Polled {} records from Kafka", records.count());

            for (ConsumerRecord<String, String> record : records) {
                KafkaMessage msg = objectMapper.readValue(record.value(), KafkaMessage.class);
                messages.add(msg);

                lastProcessedOffsets.put(new TopicPartition(record.topic(), record.partition()), record.offset());
            }
        } catch (Exception e) {
            logger.error("Error while reading Kafka messages", e);
        }
        return messages;
    }

    private String buildTargetBlobPath(String sourceSystem, Double timestamp, String batchId, String consumerRef, String processRef, String fileName) {
        // convert timestamp double to ISO string
        String timestampStr = instantToIsoString(timestamp);
        return String.format("%s/input/%s/%s/%s_%s/%s",
                sourceSystem,
                timestampStr.replace(":", "-"),  // replace colon to avoid path issues
                batchId,
                consumerRef,
                processRef,
                fileName);
    }

    private String instantToIsoString(Double timestamp) {
        // timestamp assumed to be epoch seconds in double, convert to ISO 8601 string
        long epochSeconds = timestamp.longValue();
        Instant instant = Instant.ofEpochSecond(epochSeconds);
        return instant.toString();
    }

    private String generatePdfUrl(String type, String accountNumber, String batchId) {
        return String.format("https://%s/pdfs/%s/%s_%s.%s",
                azureBlobStorageAccount, type, accountNumber, batchId,
                type.equals("html") ? "html" : "pdf");
    }

    private ApiResponse buildEmptyResponse() {
        ApiResponse response = new ApiResponse();
        response.setMessage("No new messages to process");
        response.setStatus("success");
        response.setSummaryPayload(new SummaryPayload());
        return response;
    }
}
