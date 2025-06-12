@RestController
@RequestMapping("/trigger")
public class KafkaTriggerController {

    @Autowired
    private KafkaTriggerService kafkaTriggerService;

    @GetMapping("/message")
    public ResponseEntity<?> fetchKafkaMessage() {
        try {
            Map<String, Object> response = kafkaTriggerService.readNextMessage();
            return ResponseEntity.ok(response);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(Map.of("status", "error", "error", e.getMessage()));
        }
    }
}

@Service
public class KafkaTriggerService {

    @Autowired
    private KafkaConsumer<String, String> kafkaConsumer;

    @Value("${kafka.topic.name}")
    private String topicName;

    public Map<String, Object> readNextMessage() {
        kafkaConsumer.subscribe(Collections.singletonList(topicName));
        ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(2));

        for (ConsumerRecord<String, String> record : records) {
            kafkaConsumer.commitSync(); // ensure not reprocessed

            String rawMessage = record.value();
            ObjectMapper mapper = new ObjectMapper();
            Map<String, Object> json;

            try {
                json = mapper.readValue(rawMessage, new TypeReference<>() {});
            } catch (Exception e) {
                // fallback if not valid JSON
                json = Map.of("rawMessage", rawMessage);
            }

            return Map.of("status", "success", "message", json);
        }

        return Map.of("status", "empty", "message", "No new messages available");
    }
}

@Configuration
public class KafkaConsumerConfig {

    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServers;

    @Value("${kafka.group.id}")
    private String groupId;

    @Bean
    public KafkaConsumer<String, String> kafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, 
                  "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, 
                  "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        return new KafkaConsumer<>(props);
    }
}

