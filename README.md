@Autowired
public KafkaListenerService(
        BlobStorageService blobStorageService,
        KafkaTemplate<String, String> kafkaTemplate,
        @Qualifier("auditKafkaTemplate") KafkaTemplate<String, String> auditKafkaTemplate,
        SourceSystemProperties sourceSystemProperties) {
    this.blobStorageService = blobStorageService;
    this.kafkaTemplate = kafkaTemplate; // regular topic
    this.auditKafkaTemplate = auditKafkaTemplate; // audit topic
    this.sourceSystemProperties = sourceSystemProperties;
}
