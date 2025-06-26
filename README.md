2025-06-26T12:44:16.282+02:00 ERROR 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : ❌ Failed to send metadata.json to OT [batchId=2c93525b-42d1-410a-9e26-aa957f19861d, guiRef=6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f]
org.springframework.web.client.ResourceAccessException: I/O error on POST request for "https://dev-exstream.nednet.co.za/orchestration/api/v1/inputs/ondemand/dev-SA/ECPDebtmanService": Remote host terminated the handshake
 at org.springframework.web.client.RestTemplate.createResourceAccessException(RestTemplate.java:888) ~[spring-web-6.0.2.jar!/:6.0.2]
 at org.springframework.web.client.RestTemplate.doExecute(RestTemplate.java:868) ~[spring-web-6.0.2.jar!/:6.0.2]
 at org.springframework.web.client.RestTemplate.execute(RestTemplate.java:764) ~[spring-web-6.0.2.jar!/:6.0.2]
 at org.springframework.web.client.RestTemplate.postForEntity(RestTemplate.java:512) ~[spring-web-6.0.2.jar!/:6.0.2]
 at com.nedbank.kafka.filemanage.service.KafkaListenerService.processSingleMessage(KafkaListenerService.java:97) ~[classes!/:na]
 at com.nedbank.kafka.filemanage.service.KafkaListenerService.consumeKafkaMessage(KafkaListenerService.java:48) ~[classes!/:na]
 at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[na:na]
 at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77) ~[na:na]
 at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[na:na]
 at java.base/java.lang.reflect.Method.invoke(Method.java:568) ~[na:na]
 at org.springframework.messaging.handler.invocation.InvocableHandlerMethod.doInvoke(InvocableHandlerMethod.java:169) ~[spring-messaging-6.0.2.jar!/:6.0.2]
 at org.springframework.messaging.handler.invocation.InvocableHandlerMethod.invoke(InvocableHandlerMethod.java:119) ~[spring-messaging-6.0.2.jar!/:6.0.2]
 at org.springframework.kafka.listener.adapter.HandlerAdapter.invoke(HandlerAdapter.java:56) ~[spring-kafka-3.0.11.jar!/:3.0.11]
 at org.springframework.kafka.listener.adapter.MessagingMessageListenerAdapter.invokeHandler(MessagingMessageListenerAdapter.java:375) ~[spring-kafka-3.0.11.jar!/:3.0.11]
 at org.springframework.kafka.listener.adapter.RecordMessagingMessageListenerAdapter.onMessage(RecordMessagingMessageListenerAdapter.java:92) ~[spring-kafka-3.0.11.jar!/:3.0.11]
 at org.springframework.kafka.listener.adapter.RecordMessagingMessageListenerAdapter.onMessage(RecordMessagingMessageListenerAdapter.java:53) ~[spring-kafka-3.0.11.jar!/:3.0.11]
 at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.doInvokeOnMessage(KafkaMessageListenerContainer.java:2873) ~[spring-kafka-3.0.11.jar!/:3.0.11]
 at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.invokeOnMessage(KafkaMessageListenerContainer.java:2854) ~[spring-kafka-3.0.11.jar!/:3.0.11]
 at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.lambda$doInvokeRecordListener$57(KafkaMessageListenerContainer.java:2772) ~[spring-kafka-3.0.11.jar!/:3.0.11]
 at io.micrometer.observation.Observation.observe(Observation.java:559) ~[micrometer-observation-1.10.2.jar!/:1.10.2]
 at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.doInvokeRecordListener(KafkaMessageListenerContainer.java:2770) ~[spring-kafka-3.0.11.jar!/:3.0.11]
 at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.doInvokeWithRecords(KafkaMessageListenerContainer.java:2622) ~[spring-kafka-3.0.11.jar!/:3.0.11]
 at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.invokeRecordListener(KafkaMessageListenerContainer.java:2508) ~[spring-kafka-3.0.11.jar!/:3.0.11]
 at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.invokeListener(KafkaMessageListenerContainer.java:2150) ~[spring-kafka-3.0.11.jar!/:3.0.11]
 at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.invokeIfHaveRecords(KafkaMessageListenerContainer.java:1505) ~[spring-kafka-3.0.11.jar!/:3.0.11]
 at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.pollAndInvoke(KafkaMessageListenerContainer.java:1469) ~[spring-kafka-3.0.11.jar!/:3.0.11]
 at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.run(KafkaMessageListenerContainer.java:1344) ~[spring-kafka-3.0.11.jar!/:3.0.11]
 at java.base/java.util.concurrent.CompletableFuture$AsyncRun.run(CompletableFuture.java:1804) ~[na:na]
 at java.base/java.lang.Thread.run(Thread.java:840) ~[na:na]
Caused by: javax.net.ssl.SSLHandshakeException: Remote host terminated the handshake
 at java.base/sun.security.ssl.SSLSocketImpl.handleEOF(SSLSocketImpl.java:1719) ~[na:na]
 at java.base/sun.security.ssl.SSLSocketImpl.decode(SSLSocketImpl.java:1518) ~[na:na]
 at java.base/sun.security.ssl.SSLSocketImpl.readHandshakeRecord(SSLSocketImpl.java:1425) ~[na:na]
 at java.base/sun.security.ssl.SSLSocketImpl.startHandshake(SSLSocketImpl.java:455) ~[na:na]
 at java.base/sun.security.ssl.SSLSocketImpl.startHandshake(SSLSocketImpl.java:426) ~[na:na]
 at java.base/sun.net.www.protocol.https.HttpsClient.afterConnect(HttpsClient.java:589) ~[na:na]
 at java.base/sun.net.www.protocol.https.AbstractDelegateHttpsURLConnection.connect(AbstractDelegateHttpsURLConnection.java:187) ~[na:na]
 at java.base/sun.net.www.protocol.https.HttpsURLConnectionImpl.connect(HttpsURLConnectionImpl.java:142) ~[na:na]
 at org.springframework.http.client.SimpleBufferingClientHttpRequest.executeInternal(SimpleBufferingClientHttpRequest.java:75) ~[spring-web-6.0.2.jar!/:6.0.2]
 at org.springframework.http.client.AbstractBufferingClientHttpRequest.executeInternal(AbstractBufferingClientHttpRequest.java:48) ~[spring-web-6.0.2.jar!/:6.0.2]
 at org.springframework.http.client.AbstractClientHttpRequest.execute(AbstractClientHttpRequest.java:66) ~[spring-web-6.0.2.jar!/:6.0.2]
 at org.springframework.web.client.RestTemplate.doExecute(RestTemplate.java:862) ~[spring-web-6.0.2.jar!/:6.0.2]
 ... 27 common frames omitted
Caused by: java.io.EOFException: SSL peer shut down incorrectly
 at java.base/sun.security.ssl.SSLSocketInputRecord.read(SSLSocketInputRecord.java:489) ~[na:na]
 at java.base/sun.security.ssl.SSLSocketInputRecord.readHeader(SSLSocketInputRecord.java:478) ~[na:na]
 at java.base/sun.security.ssl.SSLSocketInputRecord.decode(SSLSocketInputRecord.java:160) ~[na:na]
 at java.base/sun.security.ssl.SSLTransport.decode(SSLTransport.java:111) ~[na:na]
 at java.base/sun.security.ssl.SSLSocketImpl.decode(SSLSocketImpl.java:1510) ~[na:na]
 ... 37 common frames omitted
2025-06-26T12:44:16.383+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : ✅ Kafka message processed and sent to OT: Failed to call OT API
2025-06-26T12:44:16.383+02:00 DEBUG 1 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Committing: {str-ecp-batch-composition-0=OffsetAndMetadata{offset=18661, leaderEpoch=null, metadata=''}}


package com.nedbank.kafka.filemanage.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.nedbank.kafka.filemanage.model.ApiResponse;
import com.nedbank.kafka.filemanage.model.BatchFile;
import com.nedbank.kafka.filemanage.model.KafkaMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.io.UnsupportedEncodingException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

@Service
public class KafkaListenerService {

    private static final Logger logger = LoggerFactory.getLogger(KafkaListenerService.class);

    // Hardcoded temporarily for testing
    private static final String MOUNT_PATH_BASE = "/mnt/nfs/dev-exstream/dev-SA/jobs";
    private static final String OPENTEXT_API_URL = "https://dev-exstream.nednet.co.za/orchestration/api/v1/inputs/ondemand/dev-SA/ECPDebtmanService";

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final BlobStorageService blobStorageService;
    private final ObjectMapper objectMapper = new ObjectMapper();
    private final RestTemplate restTemplate = new RestTemplate(); // for OpenText API

    @Autowired
    public KafkaListenerService(KafkaTemplate<String, String> kafkaTemplate,
                                BlobStorageService blobStorageService) {
        this.kafkaTemplate = kafkaTemplate;
        this.blobStorageService = blobStorageService;
    }

    @KafkaListener(topics = "${kafka.topic.input}", groupId = "${kafka.consumer.group.id}",
            containerFactory = "kafkaListenerContainerFactory")
    public void consumeKafkaMessage(String message) {
        try {
            logger.info("📩 Received Kafka message.");
            KafkaMessage kafkaMessage = objectMapper.readValue(message, KafkaMessage.class);
            ApiResponse response = processSingleMessage(kafkaMessage);
            logger.info("✅ Kafka message processed and sent to OT: {}", response.getMessage());
        } catch (Exception ex) {
            logger.error("❌ Error processing Kafka message", ex);
        }
    }

    private ApiResponse processSingleMessage(KafkaMessage message) throws UnsupportedEncodingException {
        if (message == null || message.getBatchFiles() == null || message.getBatchFiles().isEmpty()) {
            return new ApiResponse("Empty or invalid message", "error", null);
        }

        List<BatchFile> validFiles = message.getBatchFiles().stream()
                .filter(f -> "DATA".equalsIgnoreCase(f.getFileType()))
                .toList();

        if (validFiles.isEmpty()) {
            return new ApiResponse("No DATA files found", "error", null);
        }

        String batchId = message.getBatchId();
        String guiRef = message.getUniqueConsumerRef();

        for (BatchFile file : validFiles) {
            String blobUrl = file.getBlobUrl();
            try {
                String fileName = extractFileName(blobUrl);
                Path localMountPath = Path.of(MOUNT_PATH_BASE, batchId, guiRef);
                Files.createDirectories(localMountPath); // safer directory creation

                Path targetFilePath = localMountPath.resolve(fileName);
                String content = blobStorageService.downloadFileContent(blobUrl);
                Files.write(targetFilePath, content.getBytes(StandardCharsets.UTF_8));

                logger.info("📁 Saved DATA file to mount: {}", targetFilePath);

                // Replace blobUrl with mount path
                file.setBlobUrl(targetFilePath.toString());

            } catch (Exception ex) {
                logger.error("❌ Failed to mount blob file [batchId={}, guiRef={}, url={}]", batchId, guiRef, blobUrl, ex);
                return new ApiResponse("Failed to mount file: " + blobUrl, "error", null);
            }
        }

        try {
            String updatedJson = objectMapper.writeValueAsString(message);
            logger.info("📤 Sending metadata.json to OpenText API at: {}", OPENTEXT_API_URL);

            restTemplate.postForEntity(OPENTEXT_API_URL, updatedJson, String.class);

            return new ApiResponse("Sent metadata to OT", "success", null);
        } catch (Exception e) {
            logger.error("❌ Failed to send metadata.json to OT [batchId={}, guiRef={}]", message.getBatchId(), message.getUniqueConsumerRef(), e);
            return new ApiResponse("Failed to call OT API", "error", null);
        }
    }

    private String extractFileName(String blobUrl) {
        if (blobUrl == null || blobUrl.isEmpty()) return "unknown.csv";
        String[] segments = blobUrl.split("/");
        return segments.length > 0 ? segments[segments.length - 1] : "unknown.csv";
    }
}
access_token : eyJraWQiOiJjZjkwMjJmMjUxNjM2MjQzNjI5YmE1ZmNmMjMwZDI4YzFlOTJkNDNiIiwidHlwIjoiSldUIiwiYWxnIjoiUlMyNTYifQ.eyJzdWIiOiIxZGY1MmRlMy1hYTJhLTQwMDUtODBmMi1jYzljMTY5NDU4ZDAiLCJzY3AiOlsib3Rkczpncm91cHMiLCJvdGRzOnJvbGVzIl0sInJvbGUiOltdLCJncnAiOlsidGVuYW50YWRtaW5zQGV4c3RyZWFtLnJvbGUiLCJvdGRzYWRtaW5zQG90ZHMuYWRtaW4iLCJvdGFkbWluc0BvdGRzLmFkbWluIiwiZW1wb3dlcmFkbWluc0BleHN0cmVhbS5yb2xlIl0sImRtcCI6eyJPVERTX0NSRURTX0FVVEgiOiJ0cnVlIiwiT1REU19IQVNfUEFTU1dPUkQiOiJmYWxzZSJ9LCJydGkiOiJiZjQxOWRiNi03OTlhLTQ4ZTAtYjhmYy01ZTFiMWQ3ODYxYmMiLCJzYXQiOjE3NDk4MDY2MjAsImlzcyI6Imh0dHBzOi8vZGV2LWV4c3RyZWFtLm5lZG5ldC5jby56YTo0NDMvb3Rkcy9vdGRzd3MiLCJncnQiOiJwYXNzd29yZCIsInN1Yl90eXAiOjAsInR5cCI6ImFjY2Vzc190b2tlbiIsInBpZCI6ImV4c3RyZWFtLnJvbGUiLCJyaWQiOnt9LCJ0aWQiOiJkZXYtZXhzdHJlYW0iLCJzaWQiOiI3MDNjYTEyYy1kNDdlLTRmOGYtOWY0OS05OWM5YWI3OWNjMDIiLCJ1aWQiOiJ0ZW5hbnRhZG1pbkBleHN0cmVhbS5yb2xlIiwidW5tIjoidGVuYW50YWRtaW4iLCJuYW1lIjoidGVuYW50YWRtaW4iLCJleHAiOjE3ODEzNDI2MjAsImlhdCI6MTc0OTgwNjYyMCwianRpIjoiOTA3YmQzMjItNDczMi00NDA0LWJiMTUtOGI5MjI1MWZiZjQ0IiwiY2lkIjoiZGV2ZXhzdHJlYW1jbGllbnQifQ.JIFEiABAISjp1uPQo-ubp4xUpxp67W4z_ynAOYywPkazTMFfniz-Tojb0uWGEilrbebIuljvjmgNfOOnrInalkaYu9-V6M4yCEWsPXJHcRB6HsqywXCgq4wB0fHGT5yCG7C9ggjNQXfSo6VPSQ5TFBaBdiFJ5H52QOwUL3rxCEkfIJx7LZDVu5Q2uEP2xRyj3dlw9kE-0cXX2cs1yM-RQUi7R4VdiGTbO9EZh7b90cTkoOSc_GX48BpDIut7835VoUzj-Qin4BAmJ25_RnOqNZ8Dwqhseahu-muw4Oo-dAVJOP5Y6ACgrNh9y3SYXgbzAd_w355kKQYk_gM3GnjOSA
