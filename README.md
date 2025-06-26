2025-06-26T13:28:46.806+02:00 DEBUG 1 --- [ntainer#0-0-C-1] .a.RecordMessagingMessageListenerAdapter : Processing [GenericMessage [payload=
                {
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
                                  "BatchFiles" : [ {
                                    "ObjectId" : "{1037A096-0000-CE1A-A484-3290CA7938C4}",
                                    "RepositoryId" : "BATCH",
                                    "BlobUrl" : "https://nsndvextr01.blob.core.windows.net/nsnakscontregecm001/DEBTMAN.csv",
                                    "Filename" : "DEBTMAN.csv",
                                    "FileType": "DATA",
                                    "ValidationStatus" : "valid"
                                  } ]
                                }
, headers={kafka_offset=18663, kafka_consumer=org.apache.kafka.clients.consumer.KafkaConsumer@2064cd66, kafka_timestampType=CREATE_TIME, kafka_receivedPartitionId=0, kafka_receivedTopic=str-ecp-batch-composition, kafka_receivedTimestamp=1750937326579, kafka_groupId=str-ecp-batch}]]
2025-06-26T13:28:46.806+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : 📩 Received Kafka message.
2025-06-26T13:28:47.308+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : 📁 Saved DATA file to mount: /mnt/nfs/dev-exstream/dev-SA/jobs/2c93525b-42d1-410a-9e26-aa957f19861d/6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f/DEBTMAN.csv
2025-06-26T13:28:47.309+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : 📤 Sending metadata.json to OpenText API at: https://dev-exstream.nednet.co.za/orchestration/api/v1/inputs/ondemand/dev-SA/ECPDebtmanService
2025-06-26T13:28:47.427+02:00 ERROR 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : ❌ Failed to send metadata.json to OT [batchId=2c93525b-42d1-410a-9e26-aa957f19861d, guiRef=6dd4dba1-8635-4bb5-8eb4-69c2aa8ccd7f]
org.springframework.web.client.ResourceAccessException: I/O error on POST request for "https://dev-exstream.nednet.co.za/orchestration/api/v1/inputs/ondemand/dev-SA/ECPDebtmanService": Remote host terminated the handshake
 at org.springframework.web.client.RestTemplate.createResourceAccessException(RestTemplate.java:888) ~[spring-web-6.0.2.jar!/:6.0.2]
 at org.springframework.web.client.RestTemplate.doExecute(RestTemplate.java:868) ~[spring-web-6.0.2.jar!/:6.0.2]
 at org.springframework.web.client.RestTemplate.execute(RestTemplate.java:764) ~[spring-web-6.0.2.jar!/:6.0.2]
 at org.springframework.web.client.RestTemplate.postForEntity(RestTemplate.java:512) ~[spring-web-6.0.2.jar!/:6.0.2]
 at com.nedbank.kafka.filemanage.service.KafkaListenerService.processSingleMessage(KafkaListenerService.java:110) ~[classes!/:na]
 at com.nedbank.kafka.filemanage.service.KafkaListenerService.consumeKafkaMessage(KafkaListenerService.java:53) ~[classes!/:na]
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
2025-06-26T13:28:47.431+02:00  INFO 1 --- [ntainer#0-0-C-1] c.n.k.f.service.KafkaListenerService     : ✅ Kafka message processed and sent to OT: Failed to call OT API
2025-06-26T13:28:47.431+02:00 DEBUG 1 --- [ntainer#0-0-C-1] o.s.k.l.KafkaMessageListenerContainer    : Committing: {str-ecp-batch-composition-0=OffsetAndMetadata{offset=18664, leaderEpoch=null, metadata=''}}
