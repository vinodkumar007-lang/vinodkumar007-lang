2025-09-18T11:45:42.862+02:00 ERROR 1 --- [ntainer#0-0-C-1] o.s.k.support.LoggingProducerListener    : Exception thrown when sending a message with key='f6a1bdb5-2fb2-4f77-a7d3-7087758a2572' and payload='{"batchId":"f6a1bdb5-2fb2-4f77-a7d3-7087758a2572","serviceName":"FmConsume","systemEnv":"Dev","sourc...' to topic log-ecp-batch-audit:

org.apache.kafka.common.errors.TimeoutException: Topic log-ecp-batch-audit not present in metadata after 180000 ms.

2025-09-18T11:45:42.868+02:00 DEBUG 1 --- [ntainer#0-0-C-1] o.s.kafka.core.KafkaTemplate             : Failed to send: ProducerRecord(topic=log-ecp-batch-audit, partition=null, headers=RecordHeaders(headers = [RecordHeader(key = X-dynaTrace, value = [0, 0, 0, 4, 120, 100, 104, 87, 0, 0, 0, 0, 0, 0, 0, 12, 0, 0, 0, 10, -63, 28, 91, 39, 20, 117, -120, -111, 0, 0, 1, 86, -93, -121, 4, 8, 0, 0, 0, 1, 6, 8, 120, 100, 104, 87, 8, 8, 0, 0, 0, 0, 10, 8, 0, 0, 0, 1, 12, 32, -34, -116, 29, 49, -71, -30, -64, 7, 81, 105, 108, -21, 56, 28, 103, 97, 14, 16, -105, 83, -112, -8, 66, -116, -106, 51]), RecordHeader(key = traceparent, value = [48, 48, 45, 100, 101, 56, 99, 49, 100, 51, 49, 98, 57, 101, 50, 99, 48, 48, 55, 53, 49, 54, 57, 54, 99, 101, 98, 51, 56, 49, 99, 54, 55, 54, 49, 45, 57, 55, 53, 51, 57, 48, 102, 56, 52, 50, 56, 99, 57, 54, 51, 51, 45, 48, 49]), RecordHeader(key = tracestate, value = [49, 52, 55, 53, 56, 56, 57, 49, 45, 99, 49, 49, 99, 53, 98, 50, 55, 64, 100, 116, 61, 102, 119, 52, 59, 97, 59, 55, 56, 54, 52, 54, 56, 53, 55, 59, 48, 59, 99, 59, 48, 59, 48, 59, 49, 53, 54, 59, 54, 50, 53, 100, 59, 50, 104, 48, 49, 59, 51, 104, 55, 56, 54, 52, 54, 56, 53, 55, 59, 52, 104, 48, 48, 59, 53, 104, 48, 49, 59, 54, 104, 100, 101, 56, 99, 49, 100, 51, 49, 98, 57, 101, 50, 99, 48, 48, 55, 53, 49, 54, 57, 54, 99, 101, 98, 51, 56, 49, 99, 54, 55, 54, 49, 59, 55, 104, 57, 55, 53, 51, 57, 48, 102, 56, 52, 50, 56, 99, 57, 54, 51, 51])], isReadOnly = false), key=f6a1bdb5-2fb2-4f77-a7d3-7087758a2572, value={"batchId":"f6a1bdb5-2fb2-4f77-a7d3-7087758a2572","serviceName":"FmConsume","systemEnv":"Dev","sourceSystem":"MFC","tenantCode":"ZANBL","channelID":"<value>","product":"MFC","jobName":"MFC","uniqueConsumerRef":"19ef9d68-b114-4803-b09b-95a6c5fa4644","timestamp":"2025-09-18T09:42:41.926775341Z","runPriority":null,"eventType":"INBOUND","startTime":"2025-09-18T09:42:41.926495838Z","endTime":"2025-09-18T09:42:41.926495838Z","customerCount":2,"batchFiles":[{"blobUrl":"/mnt/nfs/dev-exstream/dev-SA/input/MFC/f6a1bdb5-2fb2-4f77-a7d3-7087758a2572/NDDSSTBR_250902.DAT","fileName":"NDDSSTBR_250902.DAT","fileType":"REF"},{"blobUrl":"/mnt/nfs/dev-exstream/dev-SA/input/MFC/f6a1bdb5-2fb2-4f77-a7d3-7087758a2572/NDDSST_250902.DAT","fileName":"NDDSST_250902.DAT","fileType":"DATA"}]}, timestamp=null)

org.apache.kafka.common.errors.TimeoutException: Topic log-ecp-batch-audit not present in metadata after 180000 ms.

2025-09-18T11:45:42.869+02:00 ERROR 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : ❌ [batchId: f6a1bdb5-2fb2-4f77-a7d3-7087758a2572] Kafka message processing failed. Error: Send failed

org.springframework.kafka.KafkaException: Send failed
	at org.springframework.kafka.core.KafkaTemplate.doSend(KafkaTemplate.java:794) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at org.springframework.kafka.core.KafkaTemplate.observeSend(KafkaTemplate.java:754) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at org.springframework.kafka.core.KafkaTemplate.send(KafkaTemplate.java:544) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.sendToAuditTopic(KafkaListenerService.java:923) ~[classes!/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.onKafkaMessage(KafkaListenerService.java:181) ~[classes!/:na]
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

2025-09-18T11:45:42.952+02:00 DEBUG 1 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Committing: {str-ecp-batch-composition-0=OffsetAndMetadata{offset=19360, leaderEpoch=null, metadata=''}}
