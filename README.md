@Value("${kafka.topic.audit}")
    private String auditTopic;

    private final ObjectMapper objectMapper = new ObjectMapper();
    private final BlobStorageService blobStorageService;
    private final KafkaTemplate<String, String> kafkaTemplate;       // For regular topic
    private final KafkaTemplate<String, String> auditKafkaTemplate;  // For audit topic
    private final SourceSystemProperties sourceSystemProperties;
    private final RestTemplate restTemplate = new RestTemplate();
    private final ExecutorService executor = Executors.newFixedThreadPool(5);

    @Autowired
    public KafkaListenerService(
            BlobStorageService blobStorageService,
            KafkaTemplate<String, String> kafkaTemplate,                       // default template
            @Qualifier("auditKafkaTemplate") KafkaTemplate<String, String> auditKafkaTemplate,  // audit template
            SourceSystemProperties sourceSystemProperties
    ) {
        this.blobStorageService = blobStorageService;
        this.kafkaTemplate = kafkaTemplate;
        this.auditKafkaTemplate = auditKafkaTemplate;
        this.sourceSystemProperties = sourceSystemProperties;
    }
