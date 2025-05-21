// Same package declaration and imports as before

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

    public KafkaListenerService(KafkaTemplate<String, String> kafkaTemplate,
                                BlobStorageService blobStorageService,
                                ConsumerFactory<String, String> consumerFactory) {
        this.kafkaTemplate = kafkaTemplate;
        this.blobStorageService = blobStorageService;
        this.consumerFactory = consumerFactory;
    }

    public Map<String, Object> processAllMessages() {
        Consumer<String, String> consumer = consumerFactory.createConsumer();
        consumer.assign(Collections.singletonList(new TopicPartition(inputTopic, 0)));
        consumer.seekToBeginning(Collections.singletonList(new TopicPartition(inputTopic, 0)));

        Map<String, Object> response = new HashMap<>();

        try {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));
            for (ConsumerRecord<String, String> record : records) {
                Map<String, Object> result = handleMessage(record.value());
                if (result != null) return result;
            }
        } catch (Exception e) {
            logger.error("Error during Kafka message processing", e);
            return generateErrorResponse("500", "Internal Server Error while processing messages");
        } finally {
            consumer.close();
        }

        return response;
    }

    private Map<String, Object> handleMessage(String message) throws JsonProcessingException {
        JsonNode root;

        try {
            root = objectMapper.readTree(message);
        } catch (Exception e) {
            message = convertPojoToJson(message);
            try {
                root = objectMapper.readTree(message);
            } catch (Exception retryEx) {
                logger.error("Failed to parse corrected JSON", retryEx);
                return generateErrorResponse("400", "Invalid JSON format");
            }
        }

        String batchId = extractField(root, "consumerReference");
        JsonNode batchFilesNode = root.get("batchFiles");

        if (batchFilesNode == null || !batchFilesNode.isArray() || batchFilesNode.isEmpty()) {
            return generateErrorResponse("404", "No batch files found");
        }

        JsonNode firstFile = batchFilesNode.get(0);
        String filePath = firstFile.get("fileLocation").asText();
        String objectId = firstFile.get("ObjectId").asText();

        String sasUrl;
        try {
            sasUrl = blobStorageService.uploadFileAndGenerateSasUrl(filePath, batchId, objectId);
        } catch (Exception e) {
            return generateErrorResponse("453", "Error generating SAS URL");
        }

        List<CustomerSummary> customerSummaries = new ArrayList<>();
        String fileName = "";
        String jobName = "";

        Set<String> archived = new HashSet<>();
        Set<String> emailed = new HashSet<>();
        Set<String> mobstat = new HashSet<>();
        Set<String> printed = new HashSet<>();

        for (JsonNode fileNode : batchFilesNode) {
            String objId = fileNode.get("ObjectId").asText();
            String location = fileNode.get("fileLocation").asText();
            String extension = getFileExtension(location).toLowerCase();
            String customerId = objId.split("_")[0];

            if (fileNode.has("fileName")) fileName = fileNode.get("fileName").asText();
            if (fileNode.has("jobName")) jobName = fileNode.get("jobName").asText();

            CustomerSummary.FileDetail detail = new CustomerSummary.FileDetail();
            detail.setObjectId(objId);
            detail.setFileLocation(location);
            detail.setFileUrl("file://" + location);
            detail.setStatus(extension.equals(".ps") ? "failed" : "OK");
            detail.setEncrypted(isEncrypted(location, extension));
            detail.setType(determineType(location, extension));

            if (location.contains("mobstat")) mobstat.add(customerId);
            if (location.contains("archive")) archived.add(customerId);
            if (location.contains("email")) emailed.add(customerId);
            if (extension.equals(".ps")) printed.add(customerId);

            CustomerSummary customer = customerSummaries.stream()
                    .filter(c -> c.getCustomerId().equals(customerId))
                    .findFirst()
                    .orElseGet(() -> {
                        CustomerSummary c = new CustomerSummary();
                        c.setCustomerId(customerId);
                        c.setAccountNumber("");
                        c.setFiles(new ArrayList<>());
                        customerSummaries.add(c);
                        return c;
                    });

            customer.getFiles().add(detail);
        }

        SummaryFileInfo summary = new SummaryFileInfo();
        summary.setFileName(fileName);
        summary.setJobName(jobName);
        summary.setBatchId(batchId);
        summary.setTimestamp(new Date().toString());
        summary.setCustomers(customerSummaries);
        summary.setSummaryFileURL(sasUrl);

        String userHome = System.getProperty("user.home");
        File jsonFile = new File(userHome, "summary.json");

        try {
            objectMapper.writeValue(jsonFile, summary);
        } catch (IOException e) {
            return generateErrorResponse("601", "Failed to write summary file");
        }

        Map<String, Object> kafkaMsg = new HashMap<>();
        kafkaMsg.put("fileName", fileName);
        kafkaMsg.put("jobName", jobName);
        kafkaMsg.put("batchId", batchId);
        kafkaMsg.put("timestamp", new Date().toString());
        kafkaMsg.put("pdfFileURL", sasUrl);
        kafkaTemplate.send(outputTopic, batchId, objectMapper.writeValueAsString(kafkaMsg));

        // ✅ Construct enriched response using defined POJOs
        HeaderInfo headerInfo = new HeaderInfo();
        headerInfo.setSource("KafkaService");
        headerInfo.setStatus("200");
        headerInfo.setMessage("Processing Complete");

        MetaDataInfo metaData = new MetaDataInfo();
        metaData.setBatchId(batchId);
        metaData.setFileName(fileName);
        metaData.setJobName(jobName);
        metaData.setTimestamp(new Date().toString());
        metaData.setSummaryFileURL(sasUrl);
        metaData.setSummaryFilePath(jsonFile.getAbsolutePath());

        PayloadInfo payloadInfo = new PayloadInfo();
        payloadInfo.setTotalArchived(archived.size());
        payloadInfo.setTotalEmailed(emailed.size());
        payloadInfo.setTotalMobstat(mobstat.size());
        payloadInfo.setTotalPrinted(printed.size());
        payloadInfo.setTotalCustomersProcessed(customerSummaries.size());

        ProcessedFileInfo processedFileInfo = new ProcessedFileInfo();
        processedFileInfo.setProcessedFiles(customerSummaries);

        SummaryPayload summaryPayload = new SummaryPayload();
        summaryPayload.setHeader(headerInfo);
        summaryPayload.setMetadata(metaData);
        summaryPayload.setPayload(payloadInfo);
        summaryPayload.setProcessedFileInfo(processedFileInfo);

        return Map.of("summaryPayload", summaryPayload);
    }

    // helper methods: unchanged
}
