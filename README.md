java.lang.RuntimeException: ❌ Error uploading to Azure Blob or generating SAS URL
	at com.nedbank.kafka.filemanage.service.BlobStorageService.uploadFileAndGenerateSasUrl(BlobStorageService.java:96) ~[classes/:na]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.consumeKafkaMessage(KafkaListenerService.java:66) ~[classes/:na]
	at jdk.internal.reflect.GeneratedMethodAccessor5.invoke(Unknown Source) ~[na:na]
	at java.base/jdk.internal.reflect.DelegatingMethodAccessorImpl.invoke(DelegatingMethodAccessorImpl.java:43) ~[na:na]
	at java.base/java.lang.reflect.Method.invoke(Method.java:568) ~[na:na]
	at org.springframework.messaging.handler.invocation.InvocableHandlerMethod.doInvoke(InvocableHandlerMethod.java:169) ~[spring-messaging-6.0.2.jar:6.0.2]
	at org.springframework.messaging.handler.invocation.InvocableHandlerMethod.invoke(InvocableHandlerMethod.java:119) ~[spring-messaging-6.0.2.jar:6.0.2]
	at org.springframework.kafka.listener.adapter.HandlerAdapter.invoke(HandlerAdapter.java:56) ~[spring-kafka-3.0.11.jar:3.0.11]
	at org.springframework.kafka.listener.adapter.MessagingMessageListenerAdapter.invokeHandler(MessagingMessageListenerAdapter.java:375) ~[spring-kafka-3.0.11.jar:3.0.11]
	at org.springframework.kafka.listener.adapter.RecordMessagingMessageListenerAdapter.onMessage(RecordMessagingMessageListenerAdapter.java:92) ~[spring-kafka-3.0.11.jar:3.0.11]
	at org.springframework.kafka.listener.adapter.RecordMessagingMessageListenerAdapter.onMessage(RecordMessagingMessageListenerAdapter.java:53) ~[spring-kafka-3.0.11.jar:3.0.11]
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.doInvokeOnMessage(KafkaMessageListenerContainer.java:2873) ~[spring-kafka-3.0.11.jar:3.0.11]
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.invokeOnMessage(KafkaMessageListenerContainer.java:2854) ~[spring-kafka-3.0.11.jar:3.0.11]
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.lambda$doInvokeRecordListener$57(KafkaMessageListenerContainer.java:2772) ~[spring-kafka-3.0.11.jar:3.0.11]
	at io.micrometer.observation.Observation.observe(Observation.java:559) ~[micrometer-observation-1.10.2.jar:1.10.2]
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.doInvokeRecordListener(KafkaMessageListenerContainer.java:2770) ~[spring-kafka-3.0.11.jar:3.0.11]
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.doInvokeWithRecords(KafkaMessageListenerContainer.java:2622) ~[spring-kafka-3.0.11.jar:3.0.11]
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.invokeRecordListener(KafkaMessageListenerContainer.java:2508) ~[spring-kafka-3.0.11.jar:3.0.11]
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.invokeListener(KafkaMessageListenerContainer.java:2150) ~[spring-kafka-3.0.11.jar:3.0.11]
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.invokeIfHaveRecords(KafkaMessageListenerContainer.java:1505) ~[spring-kafka-3.0.11.jar:3.0.11]
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.pollAndInvoke(KafkaMessageListenerContainer.java:1469) ~[spring-kafka-3.0.11.jar:3.0.11]
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.run(KafkaMessageListenerContainer.java:1344) ~[spring-kafka-3.0.11.jar:3.0.11]
	at java.base/java.util.concurrent.CompletableFuture$AsyncRun.run(CompletableFuture.java:1804) ~[na:na]
	at java.base/java.lang.Thread.run(Thread.java:842) ~[na:na]
Caused by: java.io.IOException: Server returned HTTP response code: 409 for URL: https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv
	at java.base/sun.net.www.protocol.http.HttpURLConnection.getInputStream0(HttpURLConnection.java:2018) ~[na:na]
	at java.base/sun.net.www.protocol.http.HttpURLConnection.getInputStream(HttpURLConnection.java:1610) ~[na:na]
	at java.base/sun.net.www.protocol.https.HttpsURLConnectionImpl.getInputStream(HttpsURLConnectionImpl.java:224) ~[na:na]
	at com.nedbank.kafka.filemanage.service.BlobStorageService.uploadFileAndGenerateSasUrl(BlobStorageService.java:78) ~[classes/:na]
	... 23 common frames omitted

2025-05-14T01:35:27.078+02:00 DEBUG 19484 --- [ntainer#0-0-C-1] .a.RecordMessagingMessageListenerAdapter : Processing [GenericMessage [payload=org.springframework.kafka.support.KafkaNull@4374d554, headers={id=3a2ce01a-da22-d027-6a78-7d6e0fb5bc6c, timestamp=1747179306515}]]
2025-05-14T01:35:27.078+02:00  INFO 19484 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Kafka listener method entered.
2025-05-14T01:35:27.078+02:00  INFO 19484 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Received Kafka message: {
  "sourceSystem" : "DEBTMAN",
  "timestamp" : 1747081333.833487000,
  "batchFiles" : [ {
    "fileLocation" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv",
    "validationStatus" : "valid",
    "ObjectId" : "{1037A096-0000-CE1A-A484-3290CA7938C2}",
    "RepositoryId" : "BATCH"
  } ],
  "consumerReference" : "12345",
  "processReference" : "Test12345",
  "batchControlFileData" : null
}
