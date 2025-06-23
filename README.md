2025-06-23T06:21:28.486+02:00  WARN 12036 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Bootstrap broker localhost:9092 (id: -1 rack: null) disconnected
2025-06-23T06:21:28.627+02:00 ERROR 12036 --- [ntainer#0-0-C-1] o.s.k.support.LoggingProducerListener    : Exception thrown when sending a message with key='null' and payload='{"message":"Batch processed successfully","status":"success","summaryPayload":{"batchID":"2c93525b-4...' to topic str-ecp-batch-composition-complete:

org.apache.kafka.common.errors.TimeoutException: Topic str-ecp-batch-composition-complete not present in metadata after 60000 ms.

2025-06-23T06:21:28.631+02:00 DEBUG 12036 --- [ntainer#0-0-C-1] o.s.kafka.core.KafkaTemplate             : Failed to send: ProducerRecord(topic=str-ecp-batch-composition-complete, partition=null, headers=RecordHeaders(headers = [], isReadOnly = false), key=null, value={"message":"Batch processed successfully","status":"success","summaryPayload":{"batchID":"2c93525b-42d1-410a-9e26-aa957f19861d","fileName":"DEBTMAN.csv","header":{"tenantCode":"ZANBL","channelID":null,"audienceID":null,"timestamp":"1970-01-21T05:39:11.245Z","sourceSystem":"DEBTMAN","product":"DEBTMAN","jobName":"DEBTMAN"},"metadata":{"totalFilesProcessed":11,"processingStatus":"Completed","eventOutcomeCode":"0","eventOutcomeDescription":"Success"},"payload":{"uniqueConsumerRef":"6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f","uniqueECPBatchRef":null,"runPriority":null,"eventID":null,"eventType":null,"restartKey":null,"fileCount":11},"summaryFileURL":"https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN/2c93525b-42d1-410a-9e26-aa957f19861d/6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f/summary_2c93525b-42d1-410a-9e26-aa957f19861d.json","timestamp":"2025-06-23T04:20:28.574412800Z"}}, timestamp=null)

org.apache.kafka.common.errors.TimeoutException: Topic str-ecp-batch-composition-complete not present in metadata after 60000 ms.

2025-06-23T06:21:28.632+02:00 ERROR 12036 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Error processing Kafka message

org.springframework.kafka.KafkaException: Send failed
	at org.springframework.kafka.core.KafkaTemplate.doSend(KafkaTemplate.java:794) ~[spring-kafka-3.0.11.jar:3.0.11]
	at org.springframework.kafka.core.KafkaTemplate.observeSend(KafkaTemplate.java:754) ~[spring-kafka-3.0.11.jar:3.0.11]
	at org.springframework.kafka.core.KafkaTemplate.send(KafkaTemplate.java:538) ~[spring-kafka-3.0.11.jar:3.0.11]
	at com.nedbank.kafka.filemanage.service.KafkaListenerService.consumeKafkaMessage(KafkaListenerService.java:56) ~[classes/:na]
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke0(Native Method) ~[na:na]
	at java.base/jdk.internal.reflect.NativeMethodAccessorImpl.invoke(NativeMethodAccessorImpl.java:77) ~[na:na]
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
Caused by: org.apache.kafka.common.errors.TimeoutException: Topic str-ecp-batch-composition-complete not present in metadata after 60000 ms.

2025-06-23T06:21:28.634+02:00 DEBUG 12036 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Committing: {str-ecp-batch-composition-0=OffsetAndMetadata{offset=18638, leaderEpoch=null, metadata=''}}
2025-06-23T06:21:29.648+02:00  INFO 12036 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Node -1 disconnected.
2025-06-23T06:21:29.648+02:00  WARN 12036 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Connection to node -1 (localhost/127.0.0.1:9092) could not be established. Broker may not be available.
2025-06-23T06:21:29.649+02:00  WARN 12036 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Bootstrap broker localhost:9092 (id: -1 rack: null) disconnected
2025-06-23T06:21:30.564+02:00  INFO 12036 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Node -1 disconnected.
2025-06-23T06:21:30.564+02:00  WARN 12036 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Connection to node -1 (localhost/127.0.0.1:9092) could not be established. Broker may not be available.
2025-06-23T06:21:30.564+02:00  WARN 12036 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Bootstrap broker localhost:9092 (id: -1 rack: null) disconnected
2025-06-23T06:21:31.479+02:00  INFO 12036 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Node -1 disconnected.
2025-06-23T06:21:31.480+02:00  WARN 12036 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Connection to node -1 (localhost/127.0.0.1:9092) could not be established. Broker may not be available.
2025-06-23T06:21:31.480+02:00  WARN 12036 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Bootstrap broker localhost:9092 (id: -1 rack: null) disconnected
2025-06-23T06:21:32.441+02:00  INFO 12036 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Node -1 disconnected.
2025-06-23T06:21:32.441+02:00  WARN 12036 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Connection to node -1 (localhost/127.0.0.1:9092) could not be established. Broker may not be available.
2025-06-23T06:21:32.441+02:00  WARN 12036 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Bootstrap broker localhost:9092 (id: -1 rack: null) disconnected
2025-06-23T06:21:33.406+02:00  INFO 12036 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Node -1 disconnected.
2025-06-23T06:21:33.407+02:00  WARN 12036 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Connection to node -1 (localhost/127.0.0.1:9092) could not be established. Broker may not be available.
2025-06-23T06:21:33.407+02:00  WARN 12036 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Bootstrap broker localhost:9092 (id: -1 rack: null) disconnected
2025-06-23T06:21:33.640+02:00 DEBUG 12036 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 0 records
2025-06-23T06:21:34.269+02:00  INFO 12036 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Node -1 disconnected.
2025-06-23T06:21:34.269+02:00  WARN 12036 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Connection to node -1 (localhost/127.0.0.1:9092) could not be established. Broker may not be available.
2025-06-23T06:21:34.269+02:00  WARN 12036 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Bootstrap broker localhost:9092 (id: -1 rack: null) disconnected
2025-06-23T06:21:35.389+02:00  INFO 12036 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Node -1 disconnected.
2025-06-23T06:21:35.389+02:00  WARN 12036 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Connection to node -1 (localhost/127.0.0.1:9092) could not be established. Broker may not be available.
2025-06-23T06:21:35.389+02:00  WARN 12036 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Bootstrap broker localhost:9092 (id: -1 rack: null) disconnected
2025-06-23T06:21:36.410+02:00  INFO 12036 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Node -1 disconnected.
2025-06-23T06:21:36.410+02:00  WARN 12036 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Connection to node -1 (localhost/127.0.0.1:9092) could not be established. Broker may not be available.
2025-06-23T06:21:36.410+02:00  WARN 12036 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Bootstrap broker localhost:9092 (id: -1 rack: null) disconnected
2025-06-23T06:21:37.258+02:00  INFO 12036 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Node -1 disconnected.
2025-06-23T06:21:37.259+02:00  WARN 12036 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Connection to node -1 (localhost/127.0.0.1:9092) could not be established. Broker may not be available.
2025-06-23T06:21:37.259+02:00  WARN 12036 --- [ad | producer-1] org.apache.kafka.clients.NetworkClient   : [Producer clientId=producer-1] Bootstrap broker localhost:9092 (id: -1 rack: null) disconnected
2025-06-23T06:21:37.729+02:00 DEBUG 12036 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 1 records
2025-06-23T06:21:37.729+02:00 DEBUG 12036 --- [ntainer#0-0-C-1] .a.RecordMessagingMessageListenerAdapter : Processing [GenericMessage [payload=        {
                          "BatchId" : "2c93525b-42d1-410a-9e26-aa957f19861d",
                          "SourceSystem" : "DEBTMAN",
                          "TenantCode" : "ZANBL",
                          "ChannelID" : null,
                          "AudienceID" : null,
                          "Product" : "DEBTMAN",
                          "JobName" : "DEBTMAN",
                          "UniqueConsumerRef" : "6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f",
                          "Timestamp" : 1748351245.695410901,
                          "RunPriority" : null,
                          "EventType" : null,
                          "BatchFiles" : []
                        }

, headers={kafka_offset=18638, kafka_consumer=org.apache.kafka.clients.consumer.KafkaConsumer@161459b5, kafka_timestampType=CREATE_TIME, kafka_receivedPartitionId=0, kafka_receivedTopic=str-ecp-batch-composition, kafka_receivedTimestamp=1750652497662, kafka_groupId=str-ecp-batch}]]
2025-06-23T06:21:37.730+02:00  INFO 12036 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Received Kafka message...
2025-06-23T06:21:37.730+02:00 ERROR 12036 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : BatchFiles is empty or null. Rejecting message.
2025-06-23T06:21:37.730+02:00  WARN 12036 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : Kafka message processing returned error. Not sending to output topic. Response: Invalid message: BatchFiles is empty or null
2025-06-23T06:21:37.730+02:00 DEBUG 12036 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Committing: {str-ecp-batch-compo
