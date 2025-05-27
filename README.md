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

    public Map<String, Object> listen() {
        // Create a KafkaConsumer from the injected ConsumerFactory (configured with SSL, etc)
        try (KafkaConsumer<String, String> consumer = (KafkaConsumer<String, String>) consumerFactory.createConsumer()) {
            consumer.subscribe(Collections.singletonList(inputTopic));
            logger.info("Polling Kafka topic {} for new messages...", inputTopic);

            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(5));

            if (records.isEmpty()) {
                logger.info("No new Kafka messages found");
                Map<String, Object> emptyResponse = new HashMap<>();
                emptyResponse.put("message", "No new messages found");
                emptyResponse.put("status", "empty");
                emptyResponse.put("summaryPayload", null);
                return emptyResponse;
            }

            ApiResponse lastResponse = null;
            for (ConsumerRecord<String, String> record : records) {
                logger.info("Processing Kafka message: partition={}, offset={}", record.partition(), record.offset());

                KafkaMessage message = objectMapper.readValue(record.value(), KafkaMessage.class);
                lastResponse = processSingleMessage(message);

                String responseJson = objectMapper.writeValueAsString(lastResponse);
                kafkaTemplate.send(outputTopic, responseJson);
                logger.info("Sent processed response to Kafka topic {}", outputTopic);
            }

            Map<String, Object> responseMap = new HashMap<>();
            responseMap.put("message", lastResponse.getMessage());
            responseMap.put("status", lastResponse.getStatus());
            responseMap.put("summaryPayload", lastResponse.getSummaryPayload());
            return responseMap;

        } catch (Exception e) {
            logger.error("Error during manual Kafka consumption", e);
            Map<String, Object> errorResponse = new HashMap<>();
            errorResponse.put("message", "Error processing messages: " + e.getMessage());
            errorResponse.put("status", "error");
            errorResponse.put("summaryPayload", null);
            return errorResponse;
        }
    }
