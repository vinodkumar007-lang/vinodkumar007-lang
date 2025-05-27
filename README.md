import com.example.azure.BlobStorageService;
import com.example.model.*;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerFactory;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.time.Instant;
import java.util.*;

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

    private final File summaryFile = new File(System.getProperty("user.home"), "summary.json");
    private final Map<TopicPartition, Long> lastProcessedOffsets = new HashMap<>();

    public KafkaListenerService(KafkaTemplate<String, String> kafkaTemplate,
                                BlobStorageService blobStorageService,
                                ConsumerFactory<String, String> consumerFactory) {
        this.kafkaTemplate = kafkaTemplate;
        this.blobStorageService = blobStorageService;
        this.consumerFactory = consumerFactory;
    }

    public ApiResponse processMessages() {
        Consumer<String, String> consumer = consumerFactory.createConsumer();
        consumer.subscribe(Collections.singleton(inputTopic));

        ConsumerRecords<String, String> records = consumer.poll(java.time.Duration.ofSeconds(5));
        List<SummaryProcessedFile> processedFiles = new ArrayList<>();
        List<PrintFile> printFiles = new ArrayList<>();

        SummaryPayload payload = new SummaryPayload();
        Header header = new Header();
        Metadata metadata = new Metadata();
        Payload details = new Payload();

        for (ConsumerRecord<String, String> record : records) {
            try {
                KafkaMessage kafkaMessage = objectMapper.readValue(record.value(), KafkaMessage.class);
                logger.info("Received Kafka message: {}", kafkaMessage);

                String timestampStr = Instant.now().toString();

                for (BatchFile file : kafkaMessage.getBatchFiles()) {
                    String destinationPath = String.format("%s/input/%s/%s_%s/%s",
                        kafkaMessage.getSourceSystem(),
                        kafkaMessage.getTimestamp(),
                        kafkaMessage.getUniqueConsumerRef(),
                        kafkaMessage.getBatchId(),
                        file.getFilename()
                    );

                    String newUrl = blobStorageService.copyFileFromUrlToBlob(file.getBlobUrl(), destinationPath);

                    SummaryProcessedFile processed = new SummaryProcessedFile();
                    processed.setCustomerID("C001"); // placeholder
                    processed.setAccountNumber("1234567890"); // placeholder
                    processed.setPdfArchiveFileURL(newUrl);
                    processed.setStatusCode("OK");
                    processed.setStatusDescription("Success");
                    processedFiles.add(processed);

                    PrintFile printFile = new PrintFile();
                    printFile.setPrintFileURL(newUrl.replace(".csv", ".ps"));
                    printFiles.add(printFile);
                }

                // Fill summary fields
                header.setTenantCode(kafkaMessage.getTenantCode());
                header.setSourceSystem(kafkaMessage.getSourceSystem());
                header.setProduct(kafkaMessage.getProduct());
                header.setJobName(kafkaMessage.getJobName());
                header.setTimestamp(timestampStr);

                details.setUniqueConsumerRef(kafkaMessage.getUniqueConsumerRef());
                details.setRunPriority(kafkaMessage.getRunPriority());
                details.setEventType(kafkaMessage.getEventType());

                payload.setBatchID(kafkaMessage.getBatchId());
                payload.setHeader(header);
                payload.setMetadata(metadata);
                payload.setPayload(details);
                payload.setProcessedFiles(processedFiles);
                payload.setPrintFiles(printFiles);
                payload.setTimestamp(timestampStr);

            } catch (IOException e) {
                logger.error("Error processing Kafka message", e);
            }
        }

        try {
            objectMapper.writeValue(summaryFile, payload);
            String summaryUrl = blobStorageService.uploadSummaryJson(summaryFile);
            payload.setSummaryFileURL(summaryUrl);

            ApiResponse finalResponse = new ApiResponse("Batch processed successfully", "success", payload);
            String finalJson = objectMapper.writeValueAsString(finalResponse);
            kafkaTemplate.send(outputTopic, finalJson);
            logger.info("Final response sent to Kafka output topic: {}", outputTopic);

            return finalResponse;

        } catch (IOException e) {
            logger.error("Failed to write or upload summary.json", e);
            return new ApiResponse("Failed", "error", payload);
        }
    }
}
