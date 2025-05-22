// ... your imports
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

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

        List<Map<String, Object>> processedFilesList = new ArrayList<>();
        List<Map<String, Object>> printFilesList = new ArrayList<>();
        String fileName = "";
        String jobName = "";

        for (JsonNode fileNode : batchFilesNode) {
            String objId = fileNode.get("ObjectId").asText();
            String location = fileNode.get("fileLocation").asText();
            String extension = getFileExtension(location).toLowerCase();
            String customerId = objId.split("_")[0];
            String accountNumber = "12345678" + customerId.substring(customerId.length() - 6);

            if (fileNode.has("fileName")) fileName = fileNode.get("fileName").asText();
            if (fileNode.has("jobName")) jobName = fileNode.get("jobName").asText();

            String fileUrl = "https://" + location.replace(" ", "%20");

            Map<String, Object> customerFile = processedFilesList.stream()
                .filter(c -> c.get("customerID").equals(customerId))
                .findFirst()
                .orElseGet(() -> {
                    Map<String, Object> newCust = new HashMap<>();
                    newCust.put("customerID", customerId);
                    newCust.put("accountNumber", accountNumber);
                    processedFilesList.add(newCust);
                    return newCust;
                });

            if (location.contains("archive")) {
                customerFile.put("pdfArchiveFileURL", fileUrl);
            } else if (location.contains("mobstat") && extension.equals(".pdf")) {
                customerFile.put("pdfMobstatFileURL", fileUrl);
            } else if (location.contains("email")) {
                if (extension.equals(".pdf")) customerFile.put("pdfEmailFileURL", fileUrl);
                else if (extension.equals(".html")) customerFile.put("htmlEmailFileURL", fileUrl);
                else if (extension.equals(".txt")) customerFile.put("txtEmailFileURL", fileUrl);
            }

            if (extension.equals(".ps")) {
                Map<String, Object> printEntry = new HashMap<>();
                printEntry.put("printFileURL", fileUrl);
                printFilesList.add(printEntry);
            }

            customerFile.put("statusCode", "OK");
            customerFile.put("statusDescription", "Success");
        }

        Map<String, Object> summaryJson = new HashMap<>();
        summaryJson.put("batchID", batchId);
        summaryJson.put("fileName", fileName);

        Map<String, Object> header = new HashMap<>();
        header.put("tenantCode", extractField(root, "tenantCode"));
        header.put("channelID", extractField(root, "channelID"));
        header.put("audienceID", extractField(root, "audienceID"));
        header.put("timestamp", new Date().toInstant().toString());
        header.put("sourceSystem", extractField(root, "sourceSystem"));
        header.put("product", extractField(root, "product"));
        header.put("jobName", jobName);
        summaryJson.put("header", header);

        summaryJson.put("processedFiles", processedFilesList);
        summaryJson.put("printFiles", printFilesList);

        File summaryJsonFile = new File(System.getProperty("user.home"), "summary.json");

        try {
            objectMapper.writeValue(summaryJsonFile, summaryJson);
        } catch (IOException e) {
            return generateErrorResponse("601", "Failed to write summary.json file");
        }

        // Existing logic preserved: Kafka publish + SummaryPayload
        Map<String, Object> kafkaMsg = new HashMap<>();
        kafkaMsg.put("fileName", fileName);
        kafkaMsg.put("jobName", jobName);
        kafkaMsg.put("batchId", batchId);
        kafkaMsg.put("timestamp", new Date().toString());
        kafkaMsg.put("pdfFileURL", sasUrl);
        kafkaTemplate.send(outputTopic, batchId, objectMapper.writeValueAsString(kafkaMsg));

        HeaderInfo headerInfo = new HeaderInfo();
        MetaDataInfo metaData = new MetaDataInfo();
        PayloadInfo payloadInfo = new PayloadInfo();

        ProcessedFileInfo processedFileInfo = new ProcessedFileInfo();
        processedFileInfo.setProcessedFiles(new ArrayList<>()); // Preserve existing summaries if any

        SummaryPayload summaryPayload = new SummaryPayload();
        summaryPayload.setHeader(headerInfo);
        summaryPayload.setMetadata(metaData);
        summaryPayload.setPayload(payloadInfo);
        summaryPayload.setProcessedFileInfo(processedFileInfo);

        return Map.of(
            "summaryPayload", summaryPayload,
            "summaryFilePath", summaryJsonFile.getAbsolutePath()
        );
    }

    // existing helper methods (unchanged)...

}
