
Error starting ApplicationContext. To display the condition evaluation report re-run your application with 'debug' enabled.
2025-05-27T13:41:53.718+02:00 ERROR 12168 --- [           main] o.s.boot.SpringApplication               : Application run failed

org.springframework.beans.factory.UnsatisfiedDependencyException: Error creating bean with name 'fileProcessingController' defined in file [C:\Users\CC437236\backup\file-manager\target\classes\com\nedbank\kafka\filemanage\controller\FileProcessingController.class]: Unsatisfied dependency expressed through constructor parameter 0: Error creating bean with name 'kafkaListenerService' defined in file [C:\Users\CC437236\backup\file-manager\target\classes\com\nedbank\kafka\filemanage\service\KafkaListenerService.class]: Failed to instantiate [com.nedbank.kafka.filemanage.service.KafkaListenerService]: Constructor threw exception
	at org.springframework.beans.factory.support.ConstructorResolver.createArgumentArray(ConstructorResolver.java:793) ~[spring-beans-6.0.2.jar:6.0.2]
	at org.springframework.beans.factory.support.ConstructorResolver.autowireConstructor(ConstructorResolver.java:242) ~[spring-beans-6.0.2.jar:6.0.2]
	at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.autowireConstructor(AbstractAutowireCapableBeanFactory.java:1344) ~[spring-beans-6.0.2.jar:6.0.2]
	at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.createBeanInstance(AbstractAutowireCapableBeanFactory.java:1188) ~[spring-beans-6.0.2.jar:6.0.2]
	at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.doCreateBean(AbstractAutowireCapableBeanFactory.java:561) ~[spring-beans-6.0.2.jar:6.0.2]
	at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.createBean(AbstractAutowireCapableBeanFactory.java:521) ~[spring-beans-6.0.2.jar:6.0.2]
	at org.springframework.beans.factory.support.AbstractBeanFactory.lambda$doGetBean$0(AbstractBeanFactory.java:326) ~[spring-beans-6.0.2.jar:6.0.2]
	at org.springframework.beans.factory.support.DefaultSingletonBeanRegistry.getSingleton(DefaultSingletonBeanRegistry.java:234) ~[spring-beans-6.0.2.jar:6.0.2]
	at org.springframework.beans.factory.support.AbstractBeanFactory.doGetBean(AbstractBeanFactory.java:324) ~[spring-beans-6.0.2.jar:6.0.2]
	at org.springframework.beans.factory.support.AbstractBeanFactory.getBean(AbstractBeanFactory.java:200) ~[spring-beans-6.0.2.jar:6.0.2]
	at org.springframework.beans.factory.support.DefaultListableBeanFactory.preInstantiateSingletons(DefaultListableBeanFactory.java:961) ~[spring-beans-6.0.2.jar:6.0.2]
	at org.springframework.context.support.AbstractApplicationContext.finishBeanFactoryInitialization(AbstractApplicationContext.java:915) ~[spring-context-6.0.2.jar:6.0.2]
	at org.springframework.context.support.AbstractApplicationContext.refresh(AbstractApplicationContext.java:584) ~[spring-context-6.0.2.jar:6.0.2]
	at org.springframework.boot.web.servlet.context.ServletWebServerApplicationContext.refresh(ServletWebServerApplicationContext.java:146) ~[spring-boot-3.0.0.jar:3.0.0]
	at org.springframework.boot.SpringApplication.refresh(SpringApplication.java:730) ~[spring-boot-3.0.0.jar:3.0.0]
	at org.springframework.boot.SpringApplication.refreshContext(SpringApplication.java:432) ~[spring-boot-3.0.0.jar:3.0.0]
	at org.springframework.boot.SpringApplication.run(SpringApplication.java:308) ~[spring-boot-3.0.0.jar:3.0.0]
	at org.springframework.boot.SpringApplication.run(SpringApplication.java:1302) ~[spring-boot-3.0.0.jar:3.0.0]
	at org.springframework.boot.SpringApplication.run(SpringApplication.java:1291) ~[spring-boot-3.0.0.jar:3.0.0]
	at com.nedbank.kafka.filemanage.Application.main(Application.java:10) ~[classes/:na]
Caused by: org.springframework.beans.factory.BeanCreationException: Error creating bean with name 'kafkaListenerService' defined in file [C:\Users\CC437236\backup\file-manager\target\classes\com\nedbank\kafka\filemanage\service\KafkaListenerService.class]: Failed to instantiate [com.nedbank.kafka.filemanage.service.KafkaListenerService]: Constructor threw exception
	at org.springframework.beans.factory.support.ConstructorResolver.instantiate(ConstructorResolver.java:321) ~[spring-beans-6.0.2.jar:6.0.2]
	at org.springframework.beans.factory.support.ConstructorResolver.autowireConstructor(ConstructorResolver.java:309) ~[spring-beans-6.0.2.jar:6.0.2]
	at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.autowireConstructor(AbstractAutowireCapableBeanFactory.java:1344) ~[spring-beans-6.0.2.jar:6.0.2]
	at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.createBeanInstance(AbstractAutowireCapableBeanFactory.java:1188) ~[spring-beans-6.0.2.jar:6.0.2]
	at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.doCreateBean(AbstractAutowireCapableBeanFactory.java:561) ~[spring-beans-6.0.2.jar:6.0.2]
	at org.springframework.beans.factory.support.AbstractAutowireCapableBeanFactory.createBean(AbstractAutowireCapableBeanFactory.java:521) ~[spring-beans-6.0.2.jar:6.0.2]
	at org.springframework.beans.factory.support.AbstractBeanFactory.lambda$doGetBean$0(AbstractBeanFactory.java:326) ~[spring-beans-6.0.2.jar:6.0.2]
	at org.springframework.beans.factory.support.DefaultSingletonBeanRegistry.getSingleton(DefaultSingletonBeanRegistry.java:234) ~[spring-beans-6.0.2.jar:6.0.2]
	at org.springframework.beans.factory.support.AbstractBeanFactory.doGetBean(AbstractBeanFactory.java:324) ~[spring-beans-6.0.2.jar:6.0.2]
	at org.springframework.beans.factory.support.AbstractBeanFactory.getBean(AbstractBeanFactory.java:200) ~[spring-beans-6.0.2.jar:6.0.2]
	at org.springframework.beans.factory.config.DependencyDescriptor.resolveCandidate(DependencyDescriptor.java:254) ~[spring-beans-6.0.2.jar:6.0.2]
	at org.springframework.beans.factory.support.DefaultListableBeanFactory.doResolveDependency(DefaultListableBeanFactory.java:1405) ~[spring-beans-6.0.2.jar:6.0.2]
	at org.springframework.beans.factory.support.DefaultListableBeanFactory.resolveDependency(DefaultListableBeanFactory.java:1325) ~[spring-beans-6.0.2.jar:6.0.2]
	at org.springframework.beans.factory.support.ConstructorResolver.resolveAutowiredArgument(ConstructorResolver.java:880) ~[spring-beans-6.0.2.jar:6.0.2]
	at org.springframework.beans.factory.support.ConstructorResolver.createArgumentArray(ConstructorResolver.java:784) ~[spring-beans-6.0.2.jar:6.0.2]
	... 19 common frames omitted
Caused by: org.springframework.beans.BeanInstantiationException: Failed to instantiate [com.nedbank.kafka.filemanage.service.KafkaListenerService]: Constructor threw exception
	at org.springframework.beans.BeanUtils.instantiateClass(BeanUtils.java:223) ~[spring-beans-6.0.2.jar:6.0.2]
	at org.springframework.beans.factory.support.SimpleInstantiationStrategy.instantiate(SimpleInstantiationStrategy.java:110) ~[spring-beans-6.0.2.jar:6.0.2]
	at org.springframework.beans.factory.support.ConstructorResolver.instantiate(ConstructorResolver.java:318) ~[spring-beans-6.0.2.jar:6.0.2]
	... 33 common frames omitted
Caused by: java.lang.IllegalArgumentException: Topic collection to subscribe to cannot contain null or empty topic
	at org.apache.kafka.clients.consumer.KafkaConsumer.subscribe(KafkaConsumer.java:968) ~[kafka-clients-3.3.1.jar:na]
	at org.apache.kafka.clients.consumer.KafkaConsumer.subscribe(KafkaConsumer.java:1005) ~[kafka-clients-3.3.1.jar:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.<init>(KafkaListenerService.java:51) ~[classes/:na]
	at java.base/jdk.internal.reflect.NativeConstructorAccessorImpl.newInstance0(Native Method) ~[na:na]
	at java.base/jdk.internal.reflect.NativeConstructorAccessorImpl.newInstance(NativeConstructorAccessorImpl.java:77) ~[na:na]
	at java.base/jdk.internal.reflect.DelegatingConstructorAccessorImpl.newInstance(DelegatingConstructorAccessorImpl.java:45) ~[na:na]
	at java.base/java.lang.reflect.Constructor.newInstanceWithCaller(Constructor.java:499) ~[na:na]
	at java.base/java.lang.reflect.Constructor.newInstance(Constructor.java:480) ~[na:na]
	at org.springframework.beans.BeanUtils.instantiateClass(BeanUtils.java:210) ~[spring-beans-6.0.2.jar:6.0.2]


 package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.*;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.io.File;
import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.*;

@Service
public class KafkaListenerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final BlobStorageService blobStorageService;
    private final ObjectMapper objectMapper = new ObjectMapper();

    private final KafkaConsumer<String, String> kafkaConsumer;

    @Value("${kafka.topic.input}")
    private String inputTopic;

    @Value("${kafka.topic.output}")
    private String outputTopic;

    @Value("${azure.blob.storage.account}")
    private String azureBlobStorageAccount;

    @Autowired
    public KafkaListenerService(KafkaTemplate<String, String> kafkaTemplate,
                                BlobStorageService blobStorageService,
                                KafkaConsumer<String, String> kafkaConsumer) {
        this.kafkaTemplate = kafkaTemplate;
        this.blobStorageService = blobStorageService;
        this.kafkaConsumer = kafkaConsumer;

        // Subscribe consumer to the input topic
        this.kafkaConsumer.subscribe(Collections.singletonList(inputTopic));
    }

    /**
     * Manual trigger method - polls Kafka messages and processes them.
     */
    public void listen() {
        logger.info("Starting manual poll of Kafka messages from topic '{}'", inputTopic);

        try {
            // Poll messages with a timeout
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofSeconds(5));
            if (records.isEmpty()) {
                logger.info("No new messages found in topic '{}'", inputTopic);
                return;
            }

            for (ConsumerRecord<String, String> record : records) {
                logger.info("Processing message from topic {}, partition={}, offset={}",
                        record.topic(), record.partition(), record.offset());

                try {
                    KafkaMessage message = objectMapper.readValue(record.value(), KafkaMessage.class);
                    ApiResponse response = processSingleMessage(message);

                    // Send response to output topic
                    String responseJson = objectMapper.writeValueAsString(response);
                    kafkaTemplate.send(outputTopic, responseJson);
                    logger.info("Sent processed response to Kafka topic {}", outputTopic);

                    // Commit offset after successful processing
                    kafkaConsumer.commitSync(Collections.singletonMap(
                            new TopicPartition(record.topic(), record.partition()),
                            new org.apache.kafka.clients.consumer.OffsetAndMetadata(record.offset() + 1)
                    ));

                } catch (Exception e) {
                    logger.error("Error processing Kafka message at offset " + record.offset(), e);
                    // Decide on error handling: skip, retry, or stop processing
                }
            }

        } catch (Exception e) {
            logger.error("Error during Kafka polling", e);
        }
    }

    private ApiResponse processSingleMessage(KafkaMessage message) {
        logger.info("Processing batchId={}, sourceSystem={}", message.getBatchId(), message.getSourceSystem());

        List<SummaryProcessedFile> processedFiles = new ArrayList<>();
        List<PrintFile> printFiles = new ArrayList<>();

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

        Metadata metadata = new Metadata();
        int totalFilesProcessed = 0;

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

        PrintFile pf = new PrintFile();
        pf.setPrintFileURL("https://" + azureBlobStorageAccount + "/pdfs/mobstat/PrintFileName1_" + message.getBatchId() + ".ps");
        printFiles.add(pf);

        metadata.setTotalFilesProcessed(totalFilesProcessed);
        metadata.setProcessingStatus("Completed");
        metadata.setEventOutcomeCode("200");
        metadata.setEventOutcomeDescription("Batch processed successfully");

        SummaryPayload summaryPayload = new SummaryPayload();
        summaryPayload.setBatchID(message.getBatchId());
        summaryPayload.setFileName("summary_" + message.getBatchId() + ".json");
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
                message.getSourceSystem(), message.getBatchId());
        String summaryFileUrl = blobStorageService.uploadFile(String.valueOf(summaryFile), summaryBlobPath);
        summaryPayload.setSummaryFileURL(summaryFileUrl);

        ApiResponse apiResponse = new ApiResponse();
        apiResponse.setMessage("Batch processed successfully");
        apiResponse.setStatus("success");
        apiResponse.setSummaryPayload(summaryPayload);

        return apiResponse;
    }

    private String buildTargetBlobPath(String sourceSystem, Double timestamp, String batchId,
                                       String consumerRef, String processRef, String fileName) {
        String timestampStr = instantToIsoString(timestamp);
        return String.format("%s/input/%s/%s/%s_%s/%s",
                sourceSystem,
                timestampStr.replace(":", "-"),
                batchId,
                consumerRef,
                processRef,
                fileName);
    }

    private String instantToIsoString(Double timestamp) {
        long epochSeconds = timestamp.longValue();
        Instant instant = Instant.ofEpochSecond(epochSeconds);
        return instant.toString();
    }

    private String generatePdfUrl(String type, String accountNumber, String batchId) {
        return String.format("https://%s/pdfs/%s/%s_%s.%s",
                azureBlobStorageAccount, type, accountNumber, batchId,
                type.equals("html") ? "html" : "pdf");
    }
}
