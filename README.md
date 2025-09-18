========== AUDIT KAFKA PRODUCER CONFIG ==========
Bootstrap Servers : nbpigelpdev02.africa.nedcor.net:9093,nbpproelpdev01.africa.nedcor.net:9093,nbpinelpdev01.africa.nedcor.net:9093
Security Protocol : SSL
Truststore Location : /app/keystore/truststore.jks
Keystore Location   : nedbank1
Truststore Location : /app/keystore/truststore.jks
Keystore Location   : 3dX7y3Yz9Jv6L4F
Keystore Location   : 3dX7y3Yz9Jv6L4F
Retries             : TLSv1.2
=================================================

2025-09-18T14:39:12.161+02:00 ERROR 1 --- [ntainer#0-0-C-1] o.s.k.support.LoggingProducerListener    : Exception thrown when sending a message with key='f6a1bdb5-2fb2-4f77-a7d3-7087758a2572' and payload='{"batchId":"f6a1bdb5-2fb2-4f77-a7d3-7087758a2572","serviceName":"FmConsume","systemEnv":"Dev","sourc...' to topic log-ecp-batch-audit:

org.apache.kafka.common.errors.TimeoutException: Topic log-ecp-batch-audit not present in metadata after 180000 ms.

2025-09-18T14:39:12.166+02:00 DEBUG 1 --- [ntainer#0-0-C-1] o.s.kafka.core.KafkaTemplate             : Failed to send: ProducerRecord(topic=log-ecp-batch-audit, partition=null, headers=RecordHeaders(headers = [RecordHeader(key = X-dynaTrace, value = [0, 0, 0, 4, -105, 107, -51, 97, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 4, -63, 28, 91, 39, 20, 117, -120, -111, 0, 0, 3, -89, -37, -100, 4, 8, 0, 0, 0, 1, 6, 8, -105, 107, -51, 97, 8, 8, 0, 0, 0, 0, 10, 8, 0, 0, 0, 1, 12, 32, 38, -110, -121, -68, 11, -89, -122, 13, 47, -114, 109, 3, 31, 114, 113, -82, 14, 16, 84, 57, -94, -5, -80, 0, 103, -117]), RecordHeader(key = traceparent, value = [48, 48, 45, 50, 54, 57, 50, 56, 55, 98, 99, 48, 98, 97, 55, 56, 54, 48, 100, 50, 102, 56, 101, 54, 100, 48, 51, 49, 102, 55, 50, 55, 49, 97, 101, 45, 53, 52, 51, 57, 97, 50, 102, 98, 98, 48, 48, 48, 54, 55, 56, 98, 45, 48, 49]), RecordHeader(key = tracestate, value = [49, 52, 55, 53, 56, 56, 57, 49, 45, 99, 49, 49, 99, 53, 98, 50, 55, 64, 100, 116, 61, 102, 119, 52, 59, 52, 59, 57, 55, 54, 98, 99, 100, 54, 49, 59, 48, 59, 99, 59, 48, 59, 48, 59, 51, 97, 55, 59, 51, 55, 99, 48, 59, 50, 104, 48, 49, 59, 51, 104, 57, 55, 54, 98, 99, 100, 54, 49, 59, 52, 104, 48, 48, 59, 53, 104, 48, 49, 59, 54, 104, 50, 54, 57, 50, 56, 55, 98, 99, 48, 98, 97, 55, 56, 54, 48, 100, 50, 102, 56, 101, 54, 100, 48, 51, 49, 102, 55, 50, 55, 49, 97, 101, 59, 55, 104, 53, 52, 51, 57, 97, 50, 102, 98, 98, 48, 48, 48, 54, 55, 56, 98])], isReadOnly = false), key=f6a1bdb5-2fb2-4f77-a7d3-7087758a2572, value={"batchId":"f6a1bdb5-2fb2-4f77-a7d3-7087758a2572","serviceName":"FmConsume","systemEnv":"Dev","sourceSystem":"MFC","tenantCode":"ZANBL","channelID":"<value>","product":"MFC","jobName":"MFC","uniqueConsumerRef":"19ef9d68-b114-4803-b09b-95a6c5fa4644","timestamp":"2025-09-18T12:36:11.214956365Z","runPriority":null,"eventType":"INBOUND","startTime":"2025-09-18T12:36:11.214677161Z","endTime":"2025-09-18T12:36:11.214677161Z","customerCount":2,"batchFiles":[{"blobUrl":"/mnt/nfs/dev-exstream/dev-SA/input/MFC/f6a1bdb5-2fb2-4f77-a7d3-7087758a2572/NDDSSTBR_250902.DAT","fileName":"NDDSSTBR_250902.DAT","fileType":"REF"},{"blobUrl":"/mnt/nfs/dev-exstream/dev-SA/input/MFC/f6a1bdb5-2fb2-4f77-a7d3-7087758a2572/NDDSST_250902.DAT","fileName":"NDDSST_250902.DAT","fileType":"DATA"}]}, timestamp=null)

org.apache.kafka.common.errors.TimeoutException: Topic log-ecp-batch-audit not present in metadata after 180000 ms.

2025-09-18T14:39:12.168+02:00 ERROR 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : ❌ [batchId: f6a1bdb5-2fb2-4f77-a7d3-7087758a2572] Kafka message processing failed. Error: Send failed

org.springframework.kafka.KafkaException: Send failed
	at org.springframework.kafka.core.KafkaTemplate.doSend(KafkaTemplate.java:794) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at org.springframework.kafka.core.KafkaTemplate.observeSend(KafkaTemplate.java:754) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at org.springframework.kafka.core.KafkaTemplate.send(KafkaTemplate.java:544) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.sendToAuditTopic(KafkaListenerService.java:930) ~[classes!/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.onKafkaMessage(KafkaListenerService.java:188) ~[classes!/:na]
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
Caused by: org.apache.kafka.common.errors.TimeoutException: Topic log-ecp-batch-audit not present in metadata after 180000 ms.

2025-09-18T14:39:12.172+02:00 DEBUG 1 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Committing: {str-ecp-batch-composition-0=OffsetAndMetadata{offset=19369, leaderEpoch=null, metadata=''}}


