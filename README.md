, headers={kafka_offset=18696, kafka_consumer=org.apache.kafka.clients.consumer.KafkaConsumer@4c383914, kafka_timestampType=CREATE_TIME, kafka_receivedPartitionId=0, kafka_receivedTopic=str-ecp-batch-composition, kafka_receivedTimestamp=1752150106674, kafka_groupId=str-ecp-batch}]]
2025-07-10T14:21:47.600+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Received Kafka message.
2025-07-10T14:21:47.601+02:00 ERROR 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Failed in processing

java.nio.file.AccessDeniedException: /deployments/input
	at java.base/sun.nio.fs.UnixException.translateToIOException(UnixException.java:90) ~[na:na]
	at java.base/sun.nio.fs.UnixException.rethrowAsIOException(UnixException.java:106) ~[na:na]
	at java.base/sun.nio.fs.UnixException.rethrowAsIOException(UnixException.java:111) ~[na:na]
	at java.base/sun.nio.fs.UnixFileSystemProvider.createDirectory(UnixFileSystemProvider.java:397) ~[na:na]
	at java.base/java.nio.file.Files.createDirectory(Files.java:700) ~[na:na]
	at java.base/java.nio.file.Files.createAndCheckIsDirectory(Files.java:807) ~[na:na]
	at java.base/java.nio.file.Files.createDirectories(Files.java:793) ~[na:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.processSingleMessage(KafkaListenerService.java:111) ~[classes!/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.consumeKafkaMessage(KafkaListenerService.java:86) ~[classes!/:na]
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

2025-07-10T14:21:47.603+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Final Summary JSON: 
null
2025-07-10T14:21:47.603+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Final API Response: 
{
  "message" : "Processing failed: /deployments/input",
  "status" : "error",
  "summaryPayload" : null
}
2025-07-10T14:21:47.955+02:00  INFO 1 --- [ad | producer-1] org.apac
