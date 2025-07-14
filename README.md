2025-07-14T16:49:29.153+02:00 DEBUG 1 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Received: 1 records
2025-07-14T16:49:29.440+02:00 DEBUG 1 --- [ntainer#0-0-C-1] .a.RecordMessagingMessageListenerAdapter : Processing [GenericMessage [payload=
                {
                          "BatchId" : "6e8e56f7-a4fe-42a1-96b3-bf12d63c9142",
                          "SourceSystem" : "DEBTMAN",
                          "TenantCode" : "ZANBL",
                          "ChannelID" : null,
                          "AudienceID" : null,
                          "Product" : "DEBTMAN",
                          "JobName" : "DEBTMAN",
                          "UniqueConsumerRef" : "19ef9d68-b114-4803-b09b-ncdnc7-c8c6-d6cs",
                          "Timestamp" : 1752230622.648047900,
                          "RunPriority" : null,
                          "EventType" : null,
                          "BatchFiles" : [ {
                            "ObjectId" : "idd_90D3F897-0000-C21B-83F3-DBA708DE95F1",
                            "RepositoryId" : "BATCH",
                            "BlobUrl" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN_20250625_151841_error.TXT",
                            "Filename" : "DEBTMAN_20250625_151841_error.TXT",
                            "FileType" : "DATA",
                            "ValidationStatus" : "Valid",
                            "ValidationRequirement" : "true"
                          } ]
                        }

, headers={kafka_offset=18744, kafka_consumer=org.apache.kafka.clients.consumer.KafkaConsumer@c7cdff0, kafka_timestampType=CREATE_TIME, kafka_receivedPartitionId=0, kafka_receivedTopic=str-ecp-batch-composition, kafka_receivedTimestamp=1752504567891, kafka_groupId=str-ecp-batch}]]
2025-07-14T16:49:29.541+02:00 DEBUG 1 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Commit list: {}
2025-07-14T16:49:29.641+02:00 ERROR 1 --- [ntainer#0-0-C-1] o.s.kafka.listener.DefaultErrorHandler   : Backoff FixedBackOff{interval=0, currentAttempts=1, maxAttempts=0} exhausted for str-ecp-batch-composition-0@18744

org.springframework.kafka.listener.ListenerExecutionFailedException: invokeHandler Failed
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.decorateException(KafkaMessageListenerContainer.java:2942) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.doInvokeOnMessage(KafkaMessageListenerContainer.java:2887) ~[spring-kafka-3.0.11.jar!/:3.0.11]
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
	Suppressed: org.springframework.kafka.listener.ListenerExecutionFailedException: Restored Stack Trace
		at org.springframework.kafka.listener.adapter.MessagingMessageListenerAdapter.checkAckArg(MessagingMessageListenerAdapter.java:397) ~[spring-kafka-3.0.11.jar!/:3.0.11]
		at org.springframework.kafka.listener.adapter.MessagingMessageListenerAdapter.invokeHandler(MessagingMessageListenerAdapter.java:380) ~[spring-kafka-3.0.11.jar!/:3.0.11]
		at org.springframework.kafka.listener.adapter.RecordMessagingMessageListenerAdapter.onMessage(RecordMessagingMessageListenerAdapter.java:92) ~[spring-kafka-3.0.11.jar!/:3.0.11]
		at org.springframework.kafka.listener.adapter.RecordMessagingMessageListenerAdapter.onMessage(RecordMessagingMessageListenerAdapter.java:53) ~[spring-kafka-3.0.11.jar!/:3.0.11]
		at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.doInvokeOnMessage(KafkaMessageListenerContainer.java:2873) ~[spring-kafka-3.0.11.jar!/:3.0.11]
Caused by: java.lang.IllegalStateException: No Acknowledgment available as an argument, the listener container must have a MANUAL AckMode to populate the Acknowledgment.
	at org.springframework.kafka.listener.adapter.MessagingMessageListenerAdapter.checkAckArg(MessagingMessageListenerAdapter.java:397) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at org.springframework.kafka.listener.adapter.MessagingMessageListenerAdapter.invokeHandler(MessagingMessageListenerAdapter.java:380) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at org.springframework.kafka.listener.adapter.RecordMessagingMessageListenerAdapter.onMessage(RecordMessagingMessageListenerAdapter.java:92) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at org.springframework.kafka.listener.adapter.RecordMessagingMessageListenerAdapter.onMessage(RecordMessagingMessageListenerAdapter.java:53) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at org.springframework.kafka.listener.KafkaMessageListenerContainer$ListenerConsumer.doInvokeOnMessage(KafkaMessageListenerContainer.java:2873) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	... 12 common frames omitted
Caused by: org.springframework.messaging.converter.MessageConversionException: Cannot handle message
	... 16 common frames omitted
Caused by: org.springframework.messaging.converter.MessageConversionException: Cannot convert from [java.lang.String] to [org.springframework.kafka.support.Acknowledgment] for GenericMessage [payload=
                {
                          "BatchId" : "6e8e56f7-a4fe-42a1-96b3-bf12d63c9142",
                          "SourceSystem" : "DEBTMAN",
                          "TenantCode" : "ZANBL",
                          "ChannelID" : null,
                          "AudienceID" : null,
                          "Product" : "DEBTMAN",
                          "JobName" : "DEBTMAN",
                          "UniqueConsumerRef" : "19ef9d68-b114-4803-b09b-ncdnc7-c8c6-d6cs",
                          "Timestamp" : 1752230622.648047900,
                          "RunPriority" : null,
                          "EventType" : null,
                          "BatchFiles" : [ {
                            "ObjectId" : "idd_90D3F897-0000-C21B-83F3-DBA708DE95F1",
                            "RepositoryId" : "BATCH",
                            "BlobUrl" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN_20250625_151841_error.TXT",
                            "Filename" : "DEBTMAN_20250625_151841_error.TXT",
                            "FileType" : "DATA",
                            "ValidationStatus" : "Valid",
                            "ValidationRequirement" : "true"
                          } ]
                        }

, headers={kafka_offset=18744, kafka_consumer=org.apache.kafka.clients.consumer.KafkaConsumer@c7cdff0, kafka_timestampType=CREATE_TIME, kafka_receivedPartitionId=0, kafka_receivedTopic=str-ecp-batch-composition, kafka_receivedTimestamp=1752504567891, kafka_groupId=str-ecp-batch}]
	at org.springframework.messaging.handler.annotation.support.PayloadMethodArgumentResolver.resolveArgument(PayloadMethodArgumentResolver.java:144) ~[spring-messaging-6.0.2.jar!/:6.0.2]
	at org.springframework.kafka.annotation.KafkaNullAwarePayloadArgumentResolver.resolveArgument(KafkaNullAwarePayloadArgumentResolver.java:46) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at org.springframework.messaging.handler.invocation.HandlerMethodArgumentResolverComposite.resolveArgument(HandlerMethodArgumentResolverComposite.java:118) ~[spring-messaging-6.0.2.jar!/:6.0.2]
	at org.springframework.messaging.handler.invocation.InvocableHandlerMethod.getMethodArgumentValues(InvocableHandlerMethod.java:147) ~[spring-messaging-6.0.2.jar!/:6.0.2]
	at org.springframework.messaging.handler.invocation.InvocableHandlerMethod.invoke(InvocableHandlerMethod.java:115) ~[spring-messaging-6.0.2.jar!/:6.0.2]
	at org.springframework.kafka.listener.adapter.HandlerAdapter.invoke(HandlerAdapter.java:56) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	at org.springframework.kafka.listener.adapter.MessagingMessageListenerAdapter.invokeHandler(MessagingMessageListenerAdapter.java:375) ~[spring-kafka-3.0.11.jar!/:3.0.11]
	... 15 common frames omitted

2025-07-14T16:49:29.739+02:00 DEBUG 1 --- [ntainer#0-0-C-1] o.s.kafka.listener.DefaultErrorHandler   : Skipping seek of: str-ecp-batch-composition-0@18744
2025-07-14T16:49:29.741+02:00 DEBUG 1 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Committing: {str-ecp-batch-composition-0=OffsetAndMetadata{offset=18745, leaderEpoch=null, metadata=''}}
