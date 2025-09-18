2025-09-18T18:14:16.462+02:00 ERROR 1 --- [ntainer#0-0-C-1] o.s.k.support.LoggingProducerListener    : Exception thrown when sending a message with key='f6a1bdb5-2fb2-4f77-a7d3-7087758a2572' and payload='{"batchId":"f6a1bdb5-2fb2-4f77-a7d3-7087758a2572","serviceName":"FmConsume","systemEnv":"Dev","sourc...' to topic log-ecp-batch-audit:

org.apache.kafka.common.errors.TimeoutException: Topic log-ecp-batch-audit not present in metadata after 180000 ms.

2025-09-18T18:14:16.467+02:00 DEBUG 1 --- [ntainer#0-0-C-1] o.s.kafka.core.KafkaTemplate             : Failed to send: ProducerRecord(topic=log-ecp-batch-audit, partition=null, headers=RecordHeaders(headers = [RecordHeader(key = X-dynaTrace, value = [0, 0, 0, 4, 117, -42, 69, -46, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 8, -63, 28, 91, 39, 20, 117, -120, -111, 0, 0, 1, -89, 43, -112, 4, 8, 0, 0, 0, 1, 6, 8, 117, -42, 69, -46, 8, 8, 0, 0, 0, 0, 10, 8, 0, 0, 0, 1, 12, 32, 11, 61, 118, -17, 90, -66, 92, -107, -69, -50, -2, -53, 24, 46, -1, 53, 14, 16, 40, -122, 34, -32, -11, -57, 57, -85]), RecordHeader(key = traceparent, value = [48, 48, 45, 48, 98, 51, 100, 55, 54, 101, 102, 53, 97, 98, 101, 53, 99, 57, 53, 98, 98, 99, 101, 102, 101, 99, 98, 49, 56, 50, 101, 102, 102, 51, 53, 45, 50, 56, 56, 54, 50, 50, 101, 48, 102, 53, 99, 55, 51, 57, 97, 98, 45, 48, 49]), RecordHeader(key = tracestate, value = [49, 52, 55, 53, 56, 56, 57, 49, 45, 99, 49, 49, 99, 53, 98, 50, 55, 64, 100, 116, 61, 102, 119, 52, 59, 56, 59, 55, 53, 100, 54, 52, 53, 100, 50, 59, 48, 59, 99, 59, 48, 59, 48, 59, 49, 97, 55, 59, 98, 57, 99, 49, 59, 50, 104, 48, 49, 59, 51, 104, 55, 53, 100, 54, 52, 53, 100, 50, 59, 52, 104, 48, 48, 59, 53, 104, 48, 49, 59, 54, 104, 48, 98, 51, 100, 55, 54, 101, 102, 53, 97, 98, 101, 53, 99, 57, 53, 98, 98, 99, 101, 102, 101, 99, 98, 49, 56, 50, 101, 102, 102, 51, 53, 59, 55, 104, 50, 56, 56, 54, 50, 50, 101, 48, 102, 53, 99, 55, 51, 57, 97, 98])], isReadOnly = false), key=f6a1bdb5-2fb2-4f77-a7d3-7087758a2572, value={"batchId":"f6a1bdb5-2fb2-4f77-a7d3-7087758a2572","serviceName":"FmConsume","systemEnv":"Dev","sourceSystem":"MFC","tenantCode":"ZANBL","channelID":"<value>","product":"MFC","jobName":"MFC","uniqueConsumerRef":"19ef9d68-b114-4803-b09b-95a6c5fa4644","timestamp":"2025-09-18T16:11:15.414276885Z","runPriority":null,"eventType":"INBOUND","startTime":"2025-09-18T16:11:15.414003783Z","endTime":"2025-09-18T16:11:15.414003783Z","customerCount":2,"batchFiles":[{"blobUrl":"/mnt/nfs/dev-exstream/dev-SA/input/MFC/f6a1bdb5-2fb2-4f77-a7d3-7087758a2572/NDDSSTBR_250902.DAT","fileName":"NDDSSTBR_250902.DAT","fileType":"REF"},{"blobUrl":"/mnt/nfs/dev-exstream/dev-SA/input/MFC/f6a1bdb5-2fb2-4f77-a7d3-7087758a2572/NDDSST_250902.DAT","fileName":"NDDSST_250902.DAT","fileType":"DATA"}]}, timestamp=null)

org.apache.kafka.common.errors.TimeoutException: Topic log-ecp-batch-audit not present in metadata after 180000 ms.

2025-09-18T18:14:16.469+02:00 ERROR 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : ❌ [batchId: f6a1bdb5-2fb2-4f77-a7d3-7087758a2572] Kafka message processing failed. Error: Send failed

org.springframework.kafka.KafkaException: Send failed
	at org.springframework.kafka.core.KafkaTemplate.doSend(KafkaTemplate.java:794) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at org.springframework.kafka.core.KafkaTemplate.observeSend(KafkaTemplate.java:754) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at org.springframework.kafka.core.KafkaTemplate.send(KafkaTemplate.java:544) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.sendToAuditTopic(KafkaListenerService.java:936) ~[classes!/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.onKafkaMessage(KafkaListenerService.java:189) ~[classes!/:na]
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

2025-09-18T18:14:16.552+02:00 DEBUG 1 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Committing: {str-ecp-batch-composition-0=OffsetAndMetadata{offset=19383, leaderEpoch=null, metadata=''}}
