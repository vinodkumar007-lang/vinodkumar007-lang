@Service
public class KafkaListenerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final BlobStorageService blobStorageService;

    @Value("${kafka.topic.input}")
    private String inputTopic;

    @Value("${kafka.topic.output}")
    private String outputTopic;

    @Value("${azure.blob.storage.account}")
    private String azureBlobStorageAccount;

    @Value("${kafka.bootstrap.servers}")
    private String bootstrapServers;

    @Value("${kafka.consumer.group.id}")
    private String consumerGroupId;

    @Value("${kafka.consumer.auto.offset.reset}")
    private String autoOffsetReset;

    @Value("${kafka.consumer.enable.auto.commit}")
    private String enableAutoCommit;

    @Value("${kafka.consumer.key.deserializer}")
    private String keyDeserializer;

    @Value("${kafka.consumer.value.deserializer}")
    private String valueDeserializer;

    @Value("${kafka.consumer.security.protocol}")
    private String securityProtocol;

    @Value("${kafka.consumer.ssl.truststore.location}")
    private String truststoreLocation;

    @Value("${kafka.consumer.ssl.truststore.password}")
    private String truststorePassword;

    @Value("${kafka.consumer.ssl.keystore.location}")
    private String keystoreLocation;

    @Value("${kafka.consumer.ssl.keystore.password}")
    private String keystorePassword;

    @Value("${kafka.consumer.ssl.key.password}")
    private String keyPassword;

    @Value("${kafka.consumer.ssl.protocol}")
    private String sslProtocol;

    @Autowired
    public KafkaListenerService(KafkaTemplate<String, String> kafkaTemplate,
                                BlobStorageService blobStorageService) {
        this.kafkaTemplate = kafkaTemplate;
        this.blobStorageService = blobStorageService;
    }

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

        // continue with your existing logic using the 'props'
        ...
    }
}
kafka.bootstrap.servers=nsnxeteelpka01.nednet.co.za:9093,...
kafka.consumer.group.id=str-ecp-batch
kafka.consumer.auto.offset.reset=earliest
kafka.consumer.enable.auto.commit=false
kafka.consumer.key.deserializer=org.apache.kafka.common.serialization.StringDeserializer
kafka.consumer.value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
kafka.consumer.security.protocol=SSL
kafka.consumer.ssl.truststore.location=C:\\...\\truststore.jks
kafka.consumer.ssl.truststore.password=nedbank1
kafka.consumer.ssl.keystore.location=C:\\...\\keystore.jks
kafka.consumer.ssl.keystore.password=3dX7y3Yz9Jv6L4F
kafka.consumer.ssl.key.password=3dX7y3Yz9Jv6L4F
kafka.consumer.ssl.protocol=TLSv1.2
